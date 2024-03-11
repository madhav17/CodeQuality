from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace


class BasicTransformation:
    def remove_extra_spaces(self, df: DataFrame, column_name: str) -> DataFrame:
        df_transformed = df.withColumn(
            column_name, regexp_replace(col(column_name), "\\s+", " ")
        )
        return df_transformed

    def filter_citizen(self, df: DataFrame, column_name: str, age: int) -> DataFrame:
        df_filtered = df.filter(col(column_name) >= age)
        return df_filtered
