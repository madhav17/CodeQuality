from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as func


class SampleRead:
    def __init__(self):
        self.__spark = (
            SparkSession.builder.master("local").appName("SampleRead").getOrCreate()
        )
        self.__spark.sparkContext.setLogLevel("WARN")

    def read_from_json_file(self, path: str) -> DataFrame:
        return (
            self.__spark.read.option("inferSchema", True)
            .option("multiLine", True)
            .json(path)
        )

    def uppercase(self, df: DataFrame, name: str) -> DataFrame:
        df = df.withColumn(name, func.upper(func.col(name)))
        return df.count()

    def filter(self, df: DataFrame, column_name: str, value: int) -> DataFrame:
        return df.where(func.col(column_name) > value)

    def transformation(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "name",
            func.concat(func.col("first_name"), func.lit(" "), func.col("last_name")),
        )


if __name__ == "__main__":
    ob = SampleRead()
    df: DataFrame = ob.read_from_json_file(
        "/Users/madhavkhanna/DE_Projects/CodeQuality/src/resources/mock_data.json"
    )
    ob.uppercase(df, "first_name")
