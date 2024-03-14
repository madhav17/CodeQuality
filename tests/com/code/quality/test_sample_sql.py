import pandas as pd
import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

from src.com.code.quality.SqlExample import SqlExample
from tests.com.code.quality.TestConfig import TestConfig


class TestSqlExample(TestConfig):
    @pytest.fixture(scope="function")
    def sql_example(self):
        ob = SqlExample()
        yield ob

    def test_sql_to_spark(self, spark_fixture: SparkSession, sql_example: SqlExample):
        assert sql_example.sql_to_spark() == 1

    def test_create_read(self, spark_fixture: SparkSession, sql_example: SqlExample):
        list = [{"name": "Alice", "age": 1}, {"name": "Wonder", "age": 11}]
        expected_pdf = pd.DataFrame(list)
        sql_example.create_sql_table(list, "people")

        assert expected_pdf.equals(
            sql_example.read_sql_table("SELECT name, age FROM people")
        )
        sql_example.drop_table("people")

    def test_read_when_table_does_not_exists(
        self, spark_fixture: SparkSession, sql_example: SqlExample
    ):
        try:
            sql_example.read_sql_table("SELECT name, age FROM people")
        except Exception as ex:
            assert isinstance(ex, AnalysisException)

    def test_join(self, spark_fixture: SparkSession, sql_example: SqlExample):
        authors = [{"id": 0, "name": "Larry Wall"}]
        books = [{"id": 0, "title": "Programming Perl"}]
        authorship = [{"authorid": authors[0]["id"], "bookid": books[0]["id"]}]
        sales = [{"bookid": books[0]["id"], "amount": 1000}]

        sql_example.create_sql_table(authors, "author")
        sql_example.create_sql_table(books, "book")
        sql_example.create_sql_table(authorship, "authorship")
        sql_example.create_sql_table(sales, "sales")
        query: str = """SELECT amount FROM sales JOIN book ON sales.bookid=book.id JOIN authorship ON book.id = authorship.bookid JOIN author ON authorship.authorid = author.id"""

        df: pd.DataFrame = sql_example.read_sql_table(query)
        assert df.iloc[0, 0] == 1000
        sql_example.drop_table("author")
        sql_example.drop_table("book")
        sql_example.drop_table("authorship")
        sql_example.drop_table("sales")
