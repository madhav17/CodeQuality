from pyspark.sql import SparkSession, DataFrame
import pandas as pd
from pyspark.sql.functions import col
import pyspark.sql.functions as func


class SampleSql:
    def __init__(self):
        self.__spark = (
            SparkSession.builder.master("local").appName("SampleSql").getOrCreate()
        )
        self.__spark.sparkContext.setLogLevel("WARN")

    def sql_to_spark(self) -> int:
        df: pd.DateFrame = self.__spark.sql("SELECT 1").toPandas()
        return df.iloc[0, 0]

    def create_sql_table(self, cus_df: list, table_name: str) -> "PandasDataFrameLike":
        sdf = self.__spark.createDataFrame(cus_df)
        sdf.createOrReplaceTempView(table_name)
        pdfl: "PandasDataFrameLike" = self.__spark.sql(
            f"SELECT name, age FROM {table_name}"
        ).toPandas()
        self.__spark.catalog.dropTempView(table_name)
        return pdfl
