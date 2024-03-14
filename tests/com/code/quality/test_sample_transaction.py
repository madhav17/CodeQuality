from pyspark.sql import SparkSession, DataFrame
import pytest

# use src basically the root package to import all the test cases
from src.com.code.quality.TransactionExample import TransactionExample
from tests.com.code.quality.TestConfig import TestConfig


class TestTransactionExample(TestConfig):
    @pytest.fixture(name="ob")
    def transaction_example(self):
        ob = TransactionExample()
        yield ob

    def test_normalise_transaction_information(
        self, spark_fixture: SparkSession, ob: TransactionExample
    ):
        data = ["123-456-789", "123456789", "123456789EUR", "TEXT*?WITH.*CHARACTERS"]
        test_df = spark_fixture.createDataFrame(data, "string").toDF(
            "transaction_information"
        )

        expected = ["123456789", "123456789", "123456789EUR", "TEXTWITHCHARACTERS"]
        output = ob.normalise_transaction_information(test_df)
        assert [
            row.transaction_information_cleaned for row in output.collect()
        ] == expected

    def test_apply_debit_credit_business_classification(
        self, spark_fixture: SparkSession, ob: TransactionExample
    ):
        data = [
            "101 cr",
            "202 dr",
            "0",
        ]
        df = spark_fixture.createDataFrame(data, "string").toDF("business_line_id")
        output = ob.apply_debit_credit_business_classification(df)

        expected = ["101 CREDIT", "202 DEBIT", "0 OTHER"]
        assert [row.business_line for row in output.collect()] == expected
