#!/usr/bin/env node
import * as cdk from "@aws-cdk/core";
import * as fs from 'fs';
import * as path from 'path';
import "source-map-support/register";
import { EtlPipeline } from "./stack";
import { Source } from "./types";

const app = new cdk.App();

const stack = new EtlPipeline(app, "LCS-ETL-Pipeline", {
  moduleDir: "lib",
  sources: loadSources('../sources'),
  description: "Low Cost Sensors: ETL Pipeline"
});
cdk.Tags.of(stack).add('Project', 'lcs')

function loadSources(sourceDir: string): Source[] {
  const sourceFiles = fs.readdirSync(path.resolve(__dirname, sourceDir));
  return sourceFiles.map(
    sourceFile => require(path.join(sourceDir, sourceFile))
  );
}
