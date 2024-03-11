import unittest

from pyspark.sql import SparkSession, DataFrame


class PySparkTestCase(unittest.TestCase):

    def setUp(self):
        print("set_up_class called")
        self.spark = SparkSession.builder.appName(
            "Testing PySpark Example"
        ).getOrCreate()

    def tearDown(self):
        print("tear_down_class called")
        self.spark.stop()
