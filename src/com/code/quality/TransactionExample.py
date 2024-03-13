from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as func


class TransactionExample:
    def __init__(self):
        self.__spark = (
            SparkSession.builder.master("local")
            .appName("SampleTransaction")
            .getOrCreate()
        )
        self.__spark.sparkContext.setLogLevel("WARN")

    def normalise_transaction_information(self, transactionsDf: DataFrame) -> DataFrame:
        return transactionsDf.withColumn(
            "transaction_information_cleaned",
            func.regexp_replace(func.col("transaction_information"), r"[^A-Z0-9]+", ""),
        )

    def apply_debit_credit_business_classification(
        self,
        transactions_accounts_df: DataFrame,
    ) -> DataFrame:
        return transactions_accounts_df.withColumn(
            "business_line",
            func.when(
                func.col("business_line_id").contains("cr"),
                func.concat(
                    func.regexp_replace(func.col("business_line_id"), "cr", ""),
                    func.lit("CREDIT"),
                ),
            )
            .when(
                func.col("business_line_id").contains("dr"),
                func.concat(
                    func.regexp_replace(func.col("business_line_id"), "dr", ""),
                    func.lit("DEBIT"),
                ),
            )
            .otherwise(func.concat(func.col("business_line_id"), func.lit(" OTHER"))),
        )
