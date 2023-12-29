# python3 -m pytest --cov=lib/glue/shared --cov-report=html test/glue/shared

from awsglue.context import GlueContext, DynamicFrame
from lib.glue.shared.transformations import transform


def build_dynamic_frame(glueContext: GlueContext) -> DynamicFrame:
  values = [
    ("5a2b5ac6-890f-4e70-b90d-d11790a0761b",
     "john", "smith", "1954-12-25T06:35:22.353Z"),
    ("9ed17fea-3100-4f56-b033-3f303a1a70db",
     "jane", "doe", "1955-11-20T11:52:52.022Z"),
    ("dece4144-f0c3-429a-a050-ad4da0f83a85",
     "nick", "jones", "1989-05-29T21:16:13.658Z"),
  ]
  columns = ["id", "firstname", "lastname", "birthdate"]
  data_frame = glueContext.createDataFrame(values, columns)
  return DynamicFrame.fromDF(data_frame, glueContext, 'test_name')


def test_transform(glueContext: GlueContext):
  # Arrange
  dynamic_frame = build_dynamic_frame(glueContext)

  # Act
  result = transform(dynamic_frame)

  # Assert
  assert result.count() == 3
  first_row = result.toDF().first()
  assert first_row['birthdate'] == '1954-12-25'
  assert 'createdAt' in first_row
  assert 'lastLogin' in first_row
