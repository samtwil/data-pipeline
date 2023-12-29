import middy from '@middy/core'
import { Logger, injectLambdaContext } from '@aws-lambda-powertools/logger'
import { S3CreateEvent } from 'aws-lambda'
import { GlueClient, StartJobRunCommand } from '@aws-sdk/client-glue'

const logger = new Logger({ serviceName: 'objectCreatedTrigger' })
const glueClient = new GlueClient()

const glueJobName = process.env.GLUE_JOB_NAME

const lambdaHandler = async (event: S3CreateEvent, context: any): Promise<void> => {
  if (event.Records.length) {
    const record = event.Records[0]

    const s3Object = `s3://${record.s3.bucket.name}/${record.s3.object.key}`

    // Start Glue Job
    try {
      const response = await glueClient.send(new StartJobRunCommand({
        JobName: glueJobName,
        Arguments: {
          '--S3_OBJECT': s3Object
        }
      }))
      logger.info({message: response as any})
    } catch (error: any) {
      logger.error(error)
    }
  }
}

export const handler = middy(lambdaHandler).use(
  injectLambdaContext(logger, { logEvent: true })
)
