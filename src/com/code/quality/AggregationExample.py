from pyspark import Row
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as func


class AggregationExample:
    def sum_val(self, df: DataFrame) -> DataFrame:
        print("sum val called")
        return df.groupby("name").agg(func.sum(func.col("value")).alias("sum_val"))

    def max_val(self, df: DataFrame) -> DataFrame:
        print("max val called")
        return df.groupby("name").agg(func.max(func.col("value")).alias("max_val"))

    def sort_sum_val(self, df: DataFrame) -> list[Row]:
        print("sort sum val called")
        df_agg = self.sum_val(df)
        return df_agg.sort(func.desc("sum_val")).collect()
