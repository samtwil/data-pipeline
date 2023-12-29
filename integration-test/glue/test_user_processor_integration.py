# python3 -m pytest --cov=lib/glue --cov-report=html integration-test/glue

import os
import boto3
import signal
import subprocess
import json
from lib.glue.user_processor import process_data
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
# from moto import mock_dynamodb2

S3_BUCKET_NAME = "data-s3"
ENDPOINT_URL = "http://127.0.0.1:5000/"
S3_OBJECT = f"s3://{S3_BUCKET_NAME}/data.json"
TABLE_NAME = "Data"

def initialize_test(spark: SparkSession):
  """
  Function to setup and initialize test case execution

  Args:
      spark (SparkSession): PySpark session object

  Returns:
      process: Process object for the moto server that was started
  """
  process = subprocess.Popen(
    "moto_server -p5000",
    stdout=subprocess.PIPE,
    shell=True,
    preexec_fn=os.setsid,
  )

  # S3 bucket setup
  s3 = boto3.resource(
    "s3",
    endpoint_url=f"{ENDPOINT_URL}",
    aws_access_key_id="FakeKey",
    aws_secret_access_key="FakeSecretKey",
    aws_session_token="FakeSessionToken",
    region_name="us-east-1",
  )
  s3.create_bucket(
    Bucket=S3_BUCKET_NAME,
  )

  # DynamoDB table setup
  dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id="FakeKey",
    aws_secret_access_key="FakeSecretKey",
    aws_session_token="FakeSessionToken",
    region_name="us-east-1",
  )
  dynamodb.create_table(
    TableName=TABLE_NAME,
    KeySchema=[{'AttributeName': 'id','KeyType': 'HASH'}],
    AttributeDefinitions=[{'AttributeName': 'id','AttributeType': 'S'}],
    BillingMode='PAY_PER_REQUEST'
  )
  table = dynamodb.Table(TABLE_NAME)

  hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
  hadoop_conf.set("fs.s3a.access.key", "dummy-value")
  hadoop_conf.set("fs.s3a.secret.key", "dummy-value")
  hadoop_conf.set("fs.s3a.endpoint", ENDPOINT_URL)
  hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  hadoop_conf.set("dynamodb.endpoint", ENDPOINT_URL)
  hadoop_conf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")
  hadoop_conf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")

  values = [
    ("5a2b5ac6-890f-4e70-b90d-d11790a0761b",
     "john", "smith", "1954-12-25T06:35:22.353Z"),
    ("9ed17fea-3100-4f56-b033-3f303a1a70db",
     "jane", "doe", "1955-11-20T11:52:52.022Z"),
    ("dece4144-f0c3-429a-a050-ad4da0f83a85",
     "nick", "jones", "1989-05-29T21:16:13.658Z")
  ]
  columns = ["id", "firstname", "lastname", 'birthdate']
  df = spark.createDataFrame(values, columns)
  df.write.json(S3_OBJECT)
  return process, table


# @mock_dynamodb2
def test_user_processor(glueContext: GlueContext):
  spark = glueContext.spark_session
  process, table = initialize_test(spark)

  try:
    # from lib.glue.user_processor import process_data
    # Not sure if reimporting is important or not, try with other tests
    result = process_data(glueContext, S3_OBJECT, TABLE_NAME)

    # request_items = {
    #   f'{TABLE_NAME}':{
    #     'Keys':[
    #       {'id': {'S': '5a2b5ac6-890f-4e70-b90d-d11790a0761b'}}
    #     ]
    #   }
    # }

    print('---------HELLO---------')
    print(table.item_count)

    # item = table.get_item(
    #   Key={
    #     'id':{'S':'5a2b5ac6-890f-4e70-b90d-d11790a0761b'}
    #   }
    # )
    # print(json.dumps(item))

    # TODO: pull a sample record from ddb
    assert result == 3
  finally:
    os.killpg(os.getpgid(process.pid), signal.SIGTERM)