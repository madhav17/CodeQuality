from pyspark.sql import SparkSession, DataFrame
import pytest

# use src basically the root package to import all the test cases
from src.com.code.quality.SampleTransform import SampleTransform

ob = SampleTransform()


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.getOrCreate()


def test_transform(spark_session):
    test_df: DataFrame = spark_session.createDataFrame(
        [
            ("hobbit", "Samwise", 5),
            ("hobbit", "Billbo", 50),
            ("hobbit", "Billbo", 20),
            ("wizard", "Gandalf", 1000),
        ],
        ["that_column", "another_column", "yet_another"],
    )
    new_df = ob.transform(test_df)
    assert new_df.count() == 1
    assert new_df.toPandas().to_dict("list")["new_column"][0] == 70
