from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import StructType, StringType, StructField, IntegerType


class TransformationExample:
    def __init__(self):
        self.__spark = (
            SparkSession.builder.master("local")
            .appName("TransformationExample")
            .getOrCreate()
        )
        self.__spark.sparkContext.setLogLevel("WARN")

    def sample_data(self) -> DataFrame:
        sample_data = [
            {"name": "John    D.", "age": 30},
            {"name": "Alice   G.", "age": 25},
            {"name": "Bob  T.", "age": 35},
            {"name": "Eve   A.", "age": 28},
        ]

        return self.__spark.createDataFrame(sample_data)

        # Remove additional spaces in name

    def remove_extra_spaces(self, data_frame: DataFrame, column_name: str):
        return data_frame.withColumn(
            column_name, regexp_replace(col(column_name), "\\s+", " ")
        )


if __name__ == "__main__":
    ob = TransformationExample()
    df: DataFrame = ob.sample_data()
    df = ob.remove_extra_spaces(df, "name")
    df.show()
