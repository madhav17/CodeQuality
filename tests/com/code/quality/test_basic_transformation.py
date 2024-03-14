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
        transformed_df: DataFrame = basic_transformation.filter(original_df, "age", 60)

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
        transformed_df: DataFrame = basic_transformation.filter(
            original_df, "age", 1000
        )

        assert transformed_df.count() == 0

    def test_transform(
        self, spark_fixture: SparkSession, basic_transformation: BasicTransformation
    ):
        test_df: DataFrame = spark_fixture.createDataFrame(
            [
                ("hobbit", "Samwise", 5),
                ("hobbit", "Billbo", 50),
                ("hobbit", "Billbo", 20),
                ("wizard", "Gandalf", 1000),
            ],
            ["that_column", "another_column", "yet_another"],
        )
        new_df = basic_transformation.transform(test_df)
        assert new_df.count() == 1
        assert new_df.toPandas().to_dict("list")["new_column"][0] == 70

    def test_transformation(
        self, spark_fixture: SparkSession, basic_transformation: BasicTransformation
    ):
        input: DataFrame = spark_fixture.createDataFrame(
            [
                ("Big", "Ramy", 18),
                ("Hunter", "Labrada", 21),
                ("Nick", "Walker", 40),
            ],
            ["first_name", "last_name", "age"],
        )

        assert (
            basic_transformation.concat_transformation(input)
            .first()
            .asDict()
            .get("name")
            == "Big Ramy"
        )
