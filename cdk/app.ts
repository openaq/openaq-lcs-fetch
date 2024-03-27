#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import "source-map-support/register";
import { EtlPipeline } from "./stack";

const app = new cdk.App();

const stack = new EtlPipeline(app, "lcs-etl-pipeline", {
  description: "Low Cost Sensors: ETL Pipeline",
  fetcherModuleDir: "fetcher",
  schedulerModuleDir: "scheduler",
  sources: require('../fetcher/sources'),
  bucketName: process.env.BUCKET || 'openaq-fetches',
  lcsApi: process.env.LCS_API || 'https://api.openaq.org',
  topicArn: process.env.TOPIC_ARN
});


// const testingStack = new EtlPipeline(app, "lcs-etl-testing-pipeline", {
//   description: "Low Cost Sensors: ETL Pipeline Testing",
//   fetcherModuleDir: "fetcher",
//   schedulerModuleDir: "scheduler",
//   sources: require('../fetcher/sources'),
//   bucketName: process.env.BUCKET || 'openaq-fetches-testing',
//   lcsApi: process.env.LCS_API || 'https://openaq.org'
// });


cdk.Tags.of(stack).add('project', 'openaq')
cdk.Tags.of(stack).add('product', 'lcs-fetch')
cdk.Tags.of(stack).add('env', 'prod')
// cdk.Tags.of(testingStack).add('Project', 'lcs-testing')
