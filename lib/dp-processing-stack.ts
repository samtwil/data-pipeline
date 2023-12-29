import { Code, GlueVersion, Job, JobExecutable, PythonVersion } from '@aws-cdk/aws-glue-alpha'
import { Stack, StackProps } from 'aws-cdk-lib'
import { Construct } from 'constructs'
import { ObjectCreatedTrigger } from './compute/object-created-trigger/object-created-trigger'
import { Bucket } from 'aws-cdk-lib/aws-s3'
import { DataGenerator } from './compute/data-generator/data-generator'
import { AttributeType, Table } from 'aws-cdk-lib/aws-dynamodb'
import { Asset } from 'aws-cdk-lib/aws-s3-assets'

export class DataPipelineProcessingStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props)

    const landingZoneBucket = new Bucket(this, 'LandingZone', {
      eventBridgeEnabled: true
    })

    new DataGenerator(this, 'DataGenerator', {
      destination: landingZoneBucket
    })

    const table = new Table(this, 'TargetTable', {
      tableName: 'Users',
      partitionKey: {
        name: 'id',
        type: AttributeType.STRING,
      }
    })
    table.autoScaleWriteCapacity({
      minCapacity: 5,
      maxCapacity: 50
    }).scaleOnUtilization({ targetUtilizationPercent: 75 })

    const glueSharedCodeAsset = new Asset(this, 'GlueSharedCodeAsset', {
      path: './lib/glue'
    })

    const glueJob = new Job(this, 'DataProcessorJob', {
      jobName: 'UserDataProcessor',
      executable: JobExecutable.pythonEtl({
        glueVersion: GlueVersion.V4_0,
        pythonVersion: PythonVersion.THREE,
        script: Code.fromAsset('./lib/glue/user_processor.py'),
        extraPythonFiles: [
          Code.fromBucket(glueSharedCodeAsset.bucket, glueSharedCodeAsset.s3ObjectKey)
        ]
      }),
      defaultArguments: {
        '--TABLE_NAME': table.tableName,
        '--additional-python-modules': 'python-json-logger==2.0.7'
      }
    })
    landingZoneBucket.grantRead(glueJob)
    table.grantWriteData(glueJob)

    new ObjectCreatedTrigger(this, 'ObjectCreatedTrigger', {
      source: landingZoneBucket,
      target: glueJob
    })
  }
}
