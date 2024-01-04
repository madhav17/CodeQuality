import pytest
from pyspark.sql import SparkSession, DataFrame

# use src basically the root package to import all the test cases
from src.com.code.quality.SimpleTransformation import SimpleTransformation

ob = SimpleTransformation()


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local").getOrCreate()


def test_transformation(spark_session):
    input: DataFrame = spark_session.createDataFrame(
        [
            ("Big", "Ramy", 18),
            ("Hunter", "Labrada", 21),
            ("Nick", "Walker", 40),
        ],
        ["first_name", "last_name", "age"],
    )

    assert ob.transformation(input).first().asDict().get("name") == "Big Ramy"
