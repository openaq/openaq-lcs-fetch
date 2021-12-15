#!/usr/bin/env node
import * as cdk from "@aws-cdk/core";
import "source-map-support/register";
import { EtlPipeline } from "./stack";

const app = new cdk.App();

const stack = new EtlPipeline(app, `cac-pipeline`, {
  description: `ETL Pipeline`,
  fetcherModuleDir: "../fetcher",
  schedulerModuleDir: "../scheduler",
  sources: require('../fetcher/sources'),
  bucketName: process.env.BUCKET || 'talloaks-openaq-ingest',
  fetcherEnv: {
    API_URL: process.env.API_URL || 'https://aagsfsmu92.execute-api.us-west-2.amazonaws.com'
  },
});


cdk.Tags.of(stack).add('project', 'cac')
