#!/usr/bin/env node
import * as cdk from "@aws-cdk/core";
import "source-map-support/register";
import { EtlPipeline } from "./stack";

const app = new cdk.App();

const stack = new EtlPipeline(app, "lcs-etl-pipeline", {
  description: "Low Cost Sensors: ETL Pipeline",
  fetcherModuleDir: "fetcher",
  schedulerModuleDir: "scheduler",
  sources: require('../fetcher/sources'),
  bucketName: process.env.BUCKET || 'openaq-fetches',
  lcsApi: process.env.LCS_API || 'https://api.openaq.org'
});

cdk.Tags.of(stack).add('Project', 'lcs')
