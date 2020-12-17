#!/usr/bin/env node
import * as cdk from "@aws-cdk/core";
import "source-map-support/register";
import { EtlPipeline } from "./stack";

const app = new cdk.App();

const stack = new EtlPipeline(app, "lcs-etl-pipeline", {
  fetcherModuleDir: "lib",
  schedulerModuleDir: "scheduler",
  sources: require('../lib/sources'),
  description: "Low Cost Sensors: ETL Pipeline"
});

cdk.Tags.of(stack).add('Project', 'lcs')
