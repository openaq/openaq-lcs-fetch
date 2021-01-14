#!/usr/bin/env node
import * as cdk from "@aws-cdk/core";
import "source-map-support/register";
import { EtlPipeline } from "./stack";

const app = new cdk.App();

const stack = new EtlPipeline(app, "lcs-etl-pipeline", {
  description: "Low Cost Sensors: ETL Pipeline",
  fetcherModuleDir: "lib",
  schedulerModuleDir: "scheduler",
  sources: require('../lib/sources'),
  lcsApi: process.env.LCS_API || 'https://0jac6b9iac.execute-api.us-east-1.amazonaws.com'
});

cdk.Tags.of(stack).add('Project', 'lcs')
