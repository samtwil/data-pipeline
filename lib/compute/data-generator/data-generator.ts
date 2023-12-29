import { Duration, Stack } from 'aws-cdk-lib'
import { LayerVersion } from 'aws-cdk-lib/aws-lambda'
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs'
import { IBucket } from 'aws-cdk-lib/aws-s3'
import { Construct } from 'constructs'

export interface DataGeneratorProps {
  destination: IBucket
}

export class DataGenerator extends Construct {
  constructor(scope: Construct, id: string, props: DataGeneratorProps) {
    super(scope, id)

    // Create a Layer with Powertools for AWS Lambda (TypeScript)
    const powertoolsLayer = LayerVersion.fromLayerVersionArn(
      this,
      'PowertoolsLayer',
      `arn:aws:lambda:${Stack.of(this).region}:094274105915:layer:AWSLambdaPowertoolsTypeScript:27`
    )

    const nodeFunction = new NodejsFunction(this, 'function', {
      functionName: 'DataGenerator',
      environment: {
        'DESTINATION_BUCKET_NAME': props.destination.bucketName
      },
      timeout: Duration.seconds(10),
      memorySize: 1024,
      layers: [powertoolsLayer],
      bundling: {
        minify: true
      }
    })

    props.destination.grantPut(nodeFunction)

    // TODO: Consider running on a schedule for fun
  }
}
