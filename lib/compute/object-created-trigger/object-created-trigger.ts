import { IJob } from '@aws-cdk/aws-glue-alpha'
import { Stack } from 'aws-cdk-lib'
import { PolicyStatement } from 'aws-cdk-lib/aws-iam'
import { LayerVersion } from 'aws-cdk-lib/aws-lambda'
import { S3EventSource } from 'aws-cdk-lib/aws-lambda-event-sources'
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs'
import { Bucket, EventType, IBucket } from 'aws-cdk-lib/aws-s3'
import { Construct } from 'constructs'

export interface ObjectCreatedTriggerProps {
  source: Bucket
  target: IJob
}

export class ObjectCreatedTrigger extends Construct {
  constructor(scope: Construct, id: string, props: ObjectCreatedTriggerProps) {
    super(scope, id)

    // Create a Layer with Powertools for AWS Lambda (TypeScript)
    const powertoolsLayer = LayerVersion.fromLayerVersionArn(
      this,
      'PowertoolsLayer',
      `arn:aws:lambda:${Stack.of(this).region}:094274105915:layer:AWSLambdaPowertoolsTypeScript:27`
    )

    const nodeFunction = new NodejsFunction(this, 'function', {
      functionName: 'ObjectCreatedTrigger',
      environment: {
        GLUE_JOB_NAME: props.target.jobName
      },
      layers: [powertoolsLayer],
      bundling: {
        minify: true
      }
    })
    nodeFunction.addToRolePolicy(new PolicyStatement({
      actions: ['glue:StartJobRun'],
      resources: [props.target.jobArn]
    }))

    nodeFunction.addEventSource(new S3EventSource(props.source, {
      events: [EventType.OBJECT_CREATED]
    }))
  }
}
