from random import random
from datetime import datetime, timedelta
from awsglue import DynamicFrame
from .glue_logger import logger


def reformat_birthdate(record):
  try:
    birthdate = datetime.strptime(
      record['birthdate'], '%Y-%m-%dT%H:%M:%S.%f%z')
    record['birthdate'] = birthdate.strftime('%Y-%m-%d')
  except:
    logger.exception('Failed to reformat birthdate')

  return record


def add_created_datetime(record):
  record['createdAt'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f%z')
  return record


def add_last_login_datetime(record):
  try:
    random_seconds = (random() * 172800) + 60
    last_login_datetime = datetime.now() - timedelta(seconds=random_seconds)
    record['lastLogin'] = last_login_datetime.strftime(
      '%Y-%m-%dT%H:%M:%S.%f%z')
  except:
    logger.exception('Failed to generate last login')

  return record


def transform(dynamic_frame: DynamicFrame) -> DynamicFrame:
  logger.info('Transforming records...')

  # Remove time from birthdate
  dynamic_frame = dynamic_frame.map(f=reformat_birthdate)

  # Add new field for created time stamp
  dynamic_frame = dynamic_frame.map(f=add_created_datetime)

  # Create random field for last login date
  dynamic_frame = dynamic_frame.map(f=add_last_login_datetime)

  return dynamic_frame
