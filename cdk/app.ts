#!/usr/bin/env node
import * as cdk from '@aws-cdk/core';
import 'source-map-support/register';
import { DeployStack } from './constructs/stack';

const app = new cdk.App();
new DeployStack(app, 'DeployStack');
