import sys
import logging
from awsglue.utils import getResolvedOptions
from pythonjsonlogger import jsonlogger


def log_format(x): return ['%({0:s})s'.format(i) for i in x]


supported_keys = [
  'asctime',
  'filename',
  'levelname',
  'lineno',
  'name',
  'module',
  'funcName',
  'message',
]
custom_format = ' '.join(log_format(supported_keys))
job_name = 'local'
if ('--JOB_NANE' in sys.argv):
  args = getResolvedOptions(sys.argv, ["JOB_NAME"])
  job_name = args["JOB_NAME"]
logger = logging.getLogger(job_name)
logger.setLevel(logging.INFO)

logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(custom_format)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
