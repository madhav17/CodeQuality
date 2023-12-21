from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as func


class SimpleTransformation:
    def __init__(self):
        self.__spark = (
            SparkSession.builder.master("local")
            .appName("SimpleTransformation")
            .getOrCreate()
        )
        self.__spark.sparkContext.setLogLevel("WARN")

    def transformation(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "name",
            func.concat(func.col("first_name"), func.lit(" "), func.col("last_name")),
        )
