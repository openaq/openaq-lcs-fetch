import * as events from 'aws-cdk-lib/aws-events';
import * as eventTargets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as cdk from 'aws-cdk-lib/core';
import { execSync } from 'child_process';
import { Interval, Source } from './types';
import { Construct } from 'constructs';

export class EtlPipeline extends cdk.Stack {
  constructor(
    scope: Construct,
    id: string,
    {
      fetcherModuleDir,
      schedulerModuleDir,
      sources,
      lcsApi,
      bucketName,
      topicArn,
      ...props
    }: StackProps
  ) {
    super(scope, id, props);

    const queue = new sqs.Queue(this, 'FetcherQueue', {
      queueName: `${cdk.Stack.of(this).stackName}-fetch-queue`,
      visibilityTimeout: cdk.Duration.seconds(2880),
    });
    const bucket = s3.Bucket.fromBucketName(this, 'Data', bucketName);

    this.buildFetcherLambda({
      moduleDir: fetcherModuleDir,
      queue,
      bucket,
      lcsApi,
      topicArn,
    });
    this.buildSchedulerLambdas({
      moduleDir: schedulerModuleDir,
      queue,
      sources,
    });
  }

  private buildFetcherLambda(props: {
    moduleDir: string;
    queue: sqs.Queue;
    bucket: s3.IBucket;
    lcsApi: string;
    topicArn: string;
  }): lambda.Function {
    this.prepareNodeModules(props.moduleDir);
    const handler = new lambda.Function(this, 'Fetcher', {
      description: 'Fetch a single source for a given time period',
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(props.moduleDir),
      timeout: cdk.Duration.seconds(900),
      memorySize: 512,
      environment: {
        BUCKET: props.bucket.bucketName,
        STACK: cdk.Stack.of(this).stackName,
        LCS_API: props.lcsApi,
        TOPIC_ARN: props.topicArn,
      },
    });
    handler.addEventSource(
      new SqsEventSource(props.queue, {
        batchSize: 1,
      })
    );
    props.queue.grantConsumeMessages(handler);
    props.bucket.grantReadWrite(handler);
		handler.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ['sns:Publish'],
        resources: [props.topicArn],
      })
    );

    handler.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          'secretsmanager:DescribeSecret',
          'secretsmanager:GetSecretValue',
        ],
        resources: [
          `arn:aws:secretsmanager:*:*:secret:${
            cdk.Stack.of(this).stackName
          }/*`,
        ],
      })
    );

    return handler;
  }

  private buildSchedulerLambdas(props: {
    moduleDir: string;
    queue: sqs.Queue;
    sources: Source[];
  }): lambda.Function[] {
    const durations: Record<Interval, cdk.Duration> = {
      minute: cdk.Duration.minutes(1),
      hour: cdk.Duration.hours(1),
      day: cdk.Duration.days(1),
    };
    return Object.entries(durations).map(([interval, duration]) => {
      const scheduler = new lambda.Function(
        this,
        `${interval}Scheduler`,
        {
          description: `${interval}Scheduler`,
          runtime: lambda.Runtime.NODEJS_20_X,
          handler: 'index.handler',
          code: lambda.Code.fromAsset(props.moduleDir),
          timeout: cdk.Duration.seconds(25),
          memorySize: 128,
          environment: {
            QUEUE_URL: props.queue.queueUrl,
            SOURCES: props.sources
              .filter((source) => source.frequency === interval)
              .map((source) => source.provider)
              .join(','),
          },
        }
      );
      props.queue.grantSendMessages(scheduler);
      new events.Rule(this, `${interval}Rule`, {
        schedule: events.Schedule.rate(duration),
        targets: [new eventTargets.LambdaFunction(scheduler)],
      });
      return scheduler;
    });
  }

  /**
   * Install node_modules in module directory for the sake of easy packaging.
   * @param moduleDir string
   */
  private prepareNodeModules(moduleDir: string): void {
    const cmd = [
      'yarn',
      '--prod',
      '--frozen-lockfile',
      `--modules-folder ${moduleDir}/node_modules`,
    ].join(' ');
    execSync(cmd);
  }
}

interface StackProps extends cdk.StackProps {
  fetcherModuleDir: string;
  schedulerModuleDir: string;
  lcsApi: string;
  topicArn: string;
  bucketName: string;
  sources: Source[];
}
