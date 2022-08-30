#!/usr/bin/env node
const path = process.env.ENV ? `../.env.${process.env.ENV}` : '.env';
require('dotenv').config({ path });

import * as cdk from "@aws-cdk/core";
import "source-map-support/register";
import { EtlPipeline } from "./stack";

const app = new cdk.App();
let sources = require('../fetcher/sources')

if(process.env.DEPLOYED_SOURCES) {
  const DEPLOYED_SOURCES = process.env.DEPLOYED_SOURCES.split(",");
  sources = sources.filter((d :any)=>DEPLOYED_SOURCES.includes(d.name))
}

const id = process.env.STACK || `cac-pipeline`

const stack = new EtlPipeline(app, id, {
  description: `ETL Pipeline for ${id}`,
  sources,
  fetcherModuleDir: "../fetcher",
  schedulerModuleDir: "../scheduler",
  bucketName: process.env.BUCKET || 'talloaks-openaq-ingest',
  fetcherEnv: {
    API_URL: process.env.API_URL || 'https://aagsfsmu92.execute-api.us-west-2.amazonaws.com'
  },
});


cdk.Tags.of(stack).add('Project', 'cac')
