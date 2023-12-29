# Welcome to your CDK TypeScript project

This is a blank project for CDK development with TypeScript.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

## Useful commands

* `npm run build`   compile typescript to js
* `npm run watch`   watch for changes and compile
* `npm run test`    perform the jest unit tests
* `npx cdk deploy`  deploy this stack to your default AWS account/region
* `npx cdk diff`    compare deployed stack with current state
* `npx cdk synth`   emits the synthesized CloudFormation template
* 'sam local invoke -t cdk.out/DataPipelineIngestionStack.template.json'  Run data generator function locally
* `docker run -it -v ~/.aws:/home/glue_user/.aws -v ~/development/data-pipeline:/home/glue_user/workspace/ -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 --name glue_pyspark amazon/aws-glue-libs:glue_libs_4.0.0_image_01 pyspark`

## Tasks
https://docs.aws.amazon.com/whitepapers/latest/aws-glue-best-practices-build-efficient-data-pipeline/reference-architecture-with-the-aws-glue-product-family.html

* Define integration tests with mock services using moto
* Play with prettier and tslint
* split up main stack
* play with metrics
