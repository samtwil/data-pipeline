import { Logger, injectLambdaContext } from '@aws-lambda-powertools/logger'
import middy from '@middy/core'
import { faker } from '@faker-js/faker';
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";

const logger = new Logger({ serviceName: 'dataGenerator' });
const s3Client = new S3Client()

const destination = process.env.DESTINATION_BUCKET_NAME!

type User = {
  id: string
  firstName: string
  lastName: string
  username: string
  company: string
  birthdate: string
  streetAddress: string
  city: string
  state: string
  zip: string
}

const lambdaHandler = async (event: any, context: any): Promise<void> => {
  const users: User[] = []

  for (let i = 0; i < 5000; i++) {
    users.push({
      id: faker.string.uuid(),
      firstName: faker.person.firstName(),
      lastName: faker.person.lastName(),
      username: faker.internet.userName(),
      company: faker.company.name(),
      birthdate: faker.date.birthdate().toISOString(),
      streetAddress: faker.location.streetAddress(),
      city: faker.location.city(),
      state: faker.location.state(),
      zip: faker.location.zipCode()
    })
  }

  logger.info(`${users.length} records generated`)

  const body = users.map(u => JSON.stringify(u)).join('\n')

  // Upload data to S3
  try {
    const command = new PutObjectCommand({
      Bucket: destination,
      Key: `users/${new Date().toISOString()}.json`,
      Body: body
    })
    const response = await s3Client.send(command)
  
    logger.info({message: response as any})
  } catch (error: any) {
    logger.error(error)
  }
}

export const handler = middy(lambdaHandler).use(
  injectLambdaContext(logger, { logEvent: true })
)
