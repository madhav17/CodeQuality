from pyspark.sql import SparkSession, DataFrame
import pytest

# use src basically the root package to import all the test cases
from src.com.code.quality.SampleRead import SampleRead

ob = SampleRead()


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.getOrCreate()


def test_uppercase(spark_session):
    df: DataFrame = (
        spark_session.read.option("inferSchema", True)
        .option("multiLine", True)
        .json(
            "/Users/madhavkhanna/DE_Projects/CodeQuality/src/resources/mock_data.json"
        )
    )
    assert 56 == ob.uppercase(df, "first_name")


def test_read_from_json_file():
    df: DataFrame = ob.read_from_json_file(
        "/Users/madhavkhanna/DE_Projects/CodeQuality/src/resources/mock_data.json"
    )
    assert df is not None


def test_filter(spark_session):
    input: DataFrame = spark_session.createDataFrame(
        [
            ("Madhav", "Khanna", 32),
            ("Big", "Ramy", 18),
            ("Hunter", "Labrada", 21),
            ("Nick", "Walker", 40),
        ],
        ["first_name", "last_name", "age"],
    )

    assert ob.filter(input, "age", 19).count() == 3


def test_transformation(spark_session):
    input: DataFrame = spark_session.createDataFrame(
        [
            ("Madhav", "Khanna", 32),
            ("Big", "Ramy", 18),
            ("Hunter", "Labrada", 21),
            ("Nick", "Walker", 40),
        ],
        ["first_name", "last_name", "age"],
    )

    assert (
        ob.transformation(ob.filter(input, "age", 39)).first().asDict().get("name")
        == "Nick Walker"
    )
