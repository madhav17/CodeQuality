from pyspark.sql import SparkSession, DataFrame
import pytest


class TestConfig:
    @pytest.fixture
    def spark_fixture(self):
        spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
        yield spark
