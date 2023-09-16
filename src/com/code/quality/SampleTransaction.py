from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as func


class SampleTransaction:
    def __init__(self):
        self.__spark = (
            SparkSession.builder.master("local")
            .appName("SampleTransaction")
            .getOrCreate()
        )
        self.__spark.sparkContext.setLogLevel("WARN")

    def classify_debit_credit_transactions(
        self, transactions_df: DataFrame, accounts_df: DataFrame
    ) -> DataFrame:
        """Join transactions with account information and classify as debit/credit"""

        transactions_df = self.normalise_transaction_information(transactions_df)

        transactions_accounts_df = self.join_transactionsDf_to_accounts_df(
            transactions_df, accounts_df
        )

        transactions_accounts_df = self.apply_debit_credit_business_classification(
            transactions_accounts_df
        )

        return transactions_accounts_df

    def normalise_transaction_information(
        self, transactions_df: DataFrame
    ) -> DataFrame:
        """Remove special characters from transaction information"""
        return transactions_df.withColumn(
            "transaction_information_cleaned",
            func.regexp_replace(func.col("transaction_information"), r"[^A-Z0-9]+", ""),
        )

    def join_transactionsDf_to_accounts_df(
        self, transactions_df: DataFrame, accounts_df: DataFrame
    ) -> DataFrame:
        """Join transactions to accounts information"""
        return transactions_df.join(
            accounts_df,
            on=func.substring(func.col("transaction_information_cleaned"), 1, 9)
            == func.col("account_number"),
            how="inner",
        )

    def apply_debit_credit_business_classification(
        self,
        transactions_accounts_df: DataFrame,
    ) -> DataFrame:
        """Classify transactions as coming from debit or credit account customers"""
        # TODO: move to config file
        CREDIT_ACCOUNT_IDS = ["101", "102", "103"]
        DEBIT_ACCOUNT_IDS = ["202", "202", "203"]

        return transactions_accounts_df.withColumn(
            "business_line",
            func.when(
                func.col("business_line_id").isin(CREDIT_ACCOUNT_IDS),
                func.lit("credit"),
            )
            .when(
                func.col("business_line_id").isin(DEBIT_ACCOUNT_IDS), func.lit("debit")
            )
            .otherwise(func.lit("other")),
        )
