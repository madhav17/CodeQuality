from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as func


class SampleTransform:
    def __init__(self):
        self.__spark = (
            SparkSession.builder.master("local")
            .appName("SampleTransform")
            .getOrCreate()
        )
        self.__spark.sparkContext.setLogLevel("WARN")

    def transform(self, input_df: DataFrame) -> DataFrame:
        inter_df = (
            input_df.where(input_df["that_column"] == func.lit("hobbit"))
            .groupBy("another_column")
            .agg(func.sum("yet_another").alias("new_column"))
        )
        output_df = inter_df.select(
            "another_column",
            "new_column",
            func.when(func.col("new_column") > 10, "yes")
            .otherwise("no")
            .alias("indicator"),
        ).where(func.col("indicator") == func.lit("yes"))
        return output_df
