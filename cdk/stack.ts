import * as events from "@aws-cdk/aws-events";
import * as eventTargets from "@aws-cdk/aws-events-targets";
import * as iam from "@aws-cdk/aws-iam";
import * as lambda from "@aws-cdk/aws-lambda";
import { SqsEventSource } from "@aws-cdk/aws-lambda-event-sources";
import * as s3 from "@aws-cdk/aws-s3";
import * as sqs from "@aws-cdk/aws-sqs";
import * as cdk from "@aws-cdk/core";
import { execSync } from "child_process";
import { Interval, Source } from "./types";

export class EtlPipeline extends cdk.Stack {
  constructor(
    scope: cdk.Construct,
    id: string,
    { moduleDir, sources, ...props }: StackProps
  ) {
    super(scope, id, props);

    this.prepareNodeModules(moduleDir);

    const queue = new sqs.Queue(this, "FetcherQueue", {
      queueName: `${cdk.Stack.of(this).stackName}-fetch-queue`,
      visibilityTimeout: cdk.Duration.seconds(2880),
    });
    const bucket = new s3.Bucket(this, "Data");

    this.buildFetcherLambda({ moduleDir, queue, bucket });
    this.buildSchedulerLambdas({ moduleDir, queue, sources });
  }

  /**
   * Install node_modules in module directory for the sake of easy packaging.
   * @param moduleDir string
   */
  private prepareNodeModules(moduleDir: string): void {
    const cmd = [
      "yarn",
      "--prod",
      "--frozen-lockfile",
      `--modules-folder ${moduleDir}/node_modules`,
    ].join(" ");
    execSync(cmd);
  }

  private buildFetcherLambda(props: {
    moduleDir: string;
    queue: sqs.Queue;
    bucket: s3.IBucket;
  }): lambda.Function {
    const handler = new lambda.Function(this, "FetcherLambda", {
      description: "Fetch a single source for a given time period",
      runtime: lambda.Runtime.NODEJS_12_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset(props.moduleDir),
      timeout: cdk.Duration.seconds(900),
      memorySize: 512,
      environment: {
        BUCKET: props.bucket.bucketName,
      },
    });
    handler.addEventSource(
      new SqsEventSource(props.queue, {
        batchSize: 1,
      })
    );
    props.queue.grantConsumeMessages(handler);
    props.bucket.grantPut(handler);
    handler.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "secretsmanager:DescribeSecret",
          "secretsmanager:GetSecretValue",
        ],
        resources: [
          `arn:aws:secretsmanager:::secret:${cdk.Stack.of(this).stackName}/*`,
        ],
      })
    );
    // TODO: Enable Secrets Manager access
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
        `${interval}SchedulerLambda`,
        {
          description: `${interval}Scheduler`,
          runtime: lambda.Runtime.NODEJS_12_X,
          handler: "index.handler",
          code: lambda.Code.fromAsset(props.moduleDir),
          timeout: cdk.Duration.seconds(25),
          memorySize: 128,
          environment: {
            QUEUE: props.queue.queueName,
            SOURCES: props.sources
              .filter((source) => source.frequency === interval)
              .map((source) => source.provider)
              .join(","),
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
}

interface StackProps extends cdk.StackProps {
  moduleDir: string;
  sources: Source[];
}
