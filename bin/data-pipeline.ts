#!/usr/bin/env node
import { App } from 'aws-cdk-lib'
import 'source-map-support/register';
import { DataPipelineProcessingStack } from '../lib/dp-processing-stack'

const app = new App()
const processingStack = new DataPipelineProcessingStack(app, 'DataPipelineProcessingStack')
