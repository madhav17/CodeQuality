from pyspark.sql import SparkSession, DataFrame
import pandas as pd
from pyspark.sql.functions import col
import pyspark.sql.functions as func


class SqlExample:
    def __init__(self):
        self.__spark = (
            SparkSession.builder.master("local").appName("SampleSql").getOrCreate()
        )
        self.__spark.sparkContext.setLogLevel("WARN")

    def sql_to_spark(self) -> int:
        df: pd.DateFrame = self.__spark.sql("SELECT 1").toPandas()
        return df.iloc[0, 0]

    def create_sql_table(self, cus_df: list, table_name: str):
        sdf = self.__spark.createDataFrame(cus_df)
        sdf.createOrReplaceTempView(table_name)

    def read_sql_table(self, query: str) -> pd.DataFrame:
        df: pd.DataFrame = self.__spark.sql(query).toPandas()
        return df

    def drop_table(self, name: str):
        self.__spark.catalog.dropTempView(name)  # Delete the table
