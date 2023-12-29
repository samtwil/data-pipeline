# python3 -m pytest --cov=lib/glue --cov-report=html test/glue

import mock
import pytest
from unittest.mock import MagicMock
from lib.glue.user_processor import process_data
from awsglue.context import GlueContext


@mock.patch('awsglue.context.GlueContext')
def test_process_data_invalid_arguments(mock_glue_context: GlueContext):
  # Arrange

  # Act
  with pytest.raises(Exception) as exception:
    process_data(mock_glue_context, None, None)

  # Assert
  assert exception.value.args[0] == 'Invalid arguments'


@mock.patch('lib.glue.shared.transformations.transform')
@mock.patch('awsglue.context.GlueContext')
def test_process_data_success(mock_glue_context: GlueContext, mock_transform):
  # Arrange
  mock_dynamic_frame = MagicMock()
  mock_dynamic_frame.count.return_value = 5
  mock_glue_context.create_dynamic_frame.from_options = MagicMock(return_value=mock_dynamic_frame)
  mock_transform.return_value = mock_dynamic_frame
  mock_glue_context.write_dynamic_frame.from_options = MagicMock()

  # Act
  result = process_data(mock_glue_context, 's3://test.json', 'Test')

  # Assert
  mock_glue_context.create_dynamic_frame.from_options.assert_called_once_with(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
      "paths": ['s3://test.json']
    }
  )
  mock_transform.assert_called_once_with(mock_dynamic_frame)
  mock_glue_context.write_dynamic_frame.from_options.assert_called_once_with(
    frame=mock_dynamic_frame,
    connection_type="dynamodb",
    connection_options={
      "dynamodb.output.tableName": 'Test',
      "dynamodb.throughput.write.percent": "100.0"
    }
  )
  assert result == 5
