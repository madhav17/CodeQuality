from src.com.code.quality.BasicTransformation import BasicTransformation
from tests.com.code.quality.TestConfig import TestConfig
from pyspark.sql import SparkSession, DataFrame
from pyspark.testing.utils import assertDataFrameEqual
import pytest


class TestBasicTransformation(TestConfig):
    @pytest.fixture
    def basic_transformation(self):
        ob = BasicTransformation()
        yield ob

    def test_remove_extra_spaces(
        self, spark_fixture: SparkSession, basic_transformation: BasicTransformation
    ) -> bool:
        original_df: DataFrame = (
            spark_fixture.read.option("inferSchema", True)
            .option("multiLine", True)
            .json(
                "/Users/madhavkhanna/DE_Projects/CodeQuality/src/resources/citizen.json"
            )
        )

        # Apply the transformation function from before
        transformed_df: DataFrame = basic_transformation.remove_extra_spaces(
            original_df, "name"
        )

        expected_data = [
            {"name": "John D.", "age": 30},
            {"name": "Alice G.", "age": 25},
            {"name": "Bob T.", "age": 35},
            {"name": "Eve A.", "age": 28},
            {"name": "Ramesh Kumar", "age": 70},
            {"name": "Suresh Singh", "age": 80},
        ]

        expected_df = spark_fixture.createDataFrame(expected_data)
        return assertDataFrameEqual(transformed_df, expected_df)

    def test_filter_count_age_60(
        self, spark_fixture: SparkSession, basic_transformation: BasicTransformation
    ) -> bool:
        original_df: DataFrame = (
            spark_fixture.read.option("inferSchema", True)
            .option("multiLine", True)
            .json(
                "/Users/madhavkhanna/DE_Projects/CodeQuality/src/resources/citizen.json"
            )
        )

        # Apply the transformation function from before
        transformed_df: DataFrame = basic_transformation.filter_citizen(
            original_df, "age", 60
        )

        assert transformed_df.count() == 2

    def test_filter_count_age_1000(
        self, spark_fixture: SparkSession, basic_transformation: BasicTransformation
    ) -> bool:
        original_df: DataFrame = (
            spark_fixture.read.option("inferSchema", True)
            .option("multiLine", True)
            .json(
                "/Users/madhavkhanna/DE_Projects/CodeQuality/src/resources/citizen.json"
            )
        )

        # Apply the transformation function from before
        transformed_df: DataFrame = basic_transformation.filter_citizen(
            original_df, "age", 1000
        )

        assert transformed_df.count() == 0
