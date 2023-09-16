from pyspark.sql import SparkSession, DataFrame
import pytest
import pandas as pd

# use src basically the root package to import all the test cases
from src.com.code.quality.SampleSql import SampleSql

ob = SampleSql()


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.getOrCreate()


def test_sql_to_spark(spark_session):
    assert ob.sql_to_spark() == 1


def test_create_sql_table(spark_session):
    d = [{"name": "Alice", "age": 1}, {"name": "Wonder", "age": 11}]
    expected_pdf = pd.DataFrame(d)
    assert expected_pdf.equals(ob.create_sql_table(d, "people"))
