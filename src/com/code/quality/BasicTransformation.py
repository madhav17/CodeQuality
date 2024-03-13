from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as func


class BasicTransformation:
    def remove_extra_spaces(self, df: DataFrame, column_name: str) -> DataFrame:
        df_transformed = df.withColumn(
            column_name, regexp_replace(col(column_name), "\\s+", " ")
        )
        return df_transformed

    def filter_citizen(self, df: DataFrame, column_name: str, age: int) -> DataFrame:
        df_filtered = df.filter(col(column_name) >= age)
        return df_filtered

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

    def concat_transformation(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "name",
            func.concat(func.col("first_name"), func.lit(" "), func.col("last_name")),
        )
