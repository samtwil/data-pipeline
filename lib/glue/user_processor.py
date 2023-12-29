# ~/spark/bin/spark-submit lib/glue/user_processor.py --JOB_NAME=local --S3_OBJECT=s3://datapipelineprocessingstack-landingzone0d08f665-zqy0ecvcoyn5/users/2023-12-16T14:58:14.626Z.json --TABLE_NAME=Users

import sys
from urllib.parse import unquote
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from .shared import transformations
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue import DynamicFrame
from awsglue.job import Job
from .shared.glue_logger import logger


def load_s3_data(glueContext: GlueContext, s3_object: str) -> DynamicFrame:
  logger.info("Loading data from S3...")
  dynamic_frame = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
      "paths": [s3_object]
    }
  )
  logger.info(f"{dynamic_frame.count()} records loaded into dynamic_frame")
  return dynamic_frame


def write_to_ddb(glueContext: GlueContext, dynamic_frame: DynamicFrame, table_name: str):
  logger.info(
    f"Writing {dynamic_frame.count()} records to DynamoDB table '{table_name}'...")
  foo = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="dynamodb",
    connection_options={
      "dynamodb.output.tableName": table_name,
      "dynamodb.throughput.write.percent": "100.0"
    }
  )
  print(f'------- FOO COUNT: {foo.count()}')


def process_data(glueContext: GlueContext, s3_object: str, table_name: str) -> int:
  if not s3_object or not table_name:
    raise Exception("Invalid arguments")

  dynamic_frame = load_s3_data(glueContext, s3_object)

  dynamic_frame = transformations.transform(dynamic_frame)

  write_to_ddb(glueContext, dynamic_frame, table_name)

  return dynamic_frame.count()


if __name__ == "__main__":  # pragma: no cover
  args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_OBJECT", "TABLE_NAME"])
  spark_context = SparkContext()
  glueContext = GlueContext(spark_context)
  spark = glueContext.spark_session
  job = Job(glueContext)
  job.init(args["JOB_NAME"], args)

  s3_object = unquote(args["S3_OBJECT"])
  table_name = args["TABLE_NAME"]

  logger.info("Starting to process data...")

  process_data(glueContext, s3_object, table_name)

  logger.info("Finished processing data!")

  job.commit()
