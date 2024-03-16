from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession, DataFrame
import pytest

# use src basically the root package to import all the test cases
from src.com.code.quality.ReadExample import ReadExample

from src.com.code.quality.BasicTransformation import BasicTransformation
from tests.com.code.quality.TestConfig import TestConfig
from pyspark.sql import SparkSession, DataFrame
from pyspark.testing.utils import assertDataFrameEqual
import pytest


class TestReadExample(TestConfig):
    @pytest.fixture(name="read_example", scope="class")
    def create_read_example(self):
        print("Fixture created")
        ob = ReadExample()
        yield ob

    def test_uppercase(self, spark_fixture: SparkSession, read_example: ReadExample):
        df: DataFrame = (
            spark_fixture.read.option("inferSchema", True)
            .option("multiLine", True)
            .json(
                "/Users/madhavkhanna/DE_Projects/CodeQuality/src/resources/mock_data.json"
            )
        )
        assert "BREENA" == read_example.uppercase(df, "first_name").first()[2]

    def test_json_file(self, read_example: ReadExample):
        df: DataFrame = read_example.json_file(
            "/Users/madhavkhanna/DE_Projects/CodeQuality/src/resources/mock_data.json"
        )
        assert df is not None
        assert df.count() == 56

    def test_json_file_not_found(self, read_example: ReadExample):
        try:
            df: DataFrame = read_example.json_file("random.json")
        except Exception as e:
            assert isinstance(e, AnalysisException)

    def test_filter(self, spark_fixture: SparkSession, read_example: ReadExample):
        input: DataFrame = spark_fixture.createDataFrame(
            [
                ("Madhav", "Khanna", 33),
                ("Big", "Ramy", 18),
                ("Hunter", "Labrada", 21),
                ("Nick", "Walker", 40),
            ],
            ["first_name", "last_name", "age"],
        )

        assert read_example.filter(input, "age", 19).count() == 3

    def test_transformation(
        self, spark_fixture: SparkSession, read_example: ReadExample
    ):
        input: DataFrame = spark_fixture.createDataFrame(
            [
                ("Madhav", "Khanna", 33),
                ("Big", "Ramy", 18),
                ("Hunter", "Labrada", 21),
                ("Nick", "Walker", 40),
            ],
            ["first_name", "last_name", "age"],
        )

        assert (
            read_example.transformation(read_example.filter(input, "age", 39))
            .first()
            .asDict()
            .get("name")
            == "Nick Walker"
        )
