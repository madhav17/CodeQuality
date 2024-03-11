from pyspark.sql import SparkSession, DataFrame
import pytest

from src.com.code.quality.TransformationExample import TransformationExample
from pyspark.testing.utils import assertDataFrameEqual


class TestTransformation:

    @pytest.fixture
    def spark_fixture(self):
        spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
        yield spark

    @pytest.fixture
    def transformation_example(self):
        ob = TransformationExample()
        yield ob

    def test_single_space(self, spark_fixture: SparkSession, transformation_example: TransformationExample) -> bool:
        sample_data = [{"name": "John    D.", "age": 30},
                       {"name": "Alice   G.", "age": 25},
                       {"name": "Bob  T.", "age": 35},
                       {"name": "Eve   A.", "age": 28}]

        # Create a Spark DataFrame
        original_df = spark_fixture.createDataFrame(sample_data)

        # Apply the transformation function from before
        transformed_df = transformation_example.remove_extra_spaces(original_df, "name")

        expected_data = [{"name": "John D.", "age": 30},
                         {"name": "Alice G.", "age": 25},
                         {"name": "Bob T.", "age": 35},
                         {"name": "Eve A.", "age": 28}]

        expected_df = spark_fixture.createDataFrame(expected_data)
        return assertDataFrameEqual(transformed_df, expected_df)
