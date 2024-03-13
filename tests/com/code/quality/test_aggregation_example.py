from unittest.mock import MagicMock

from pyspark.sql import SparkSession, DataFrame
import pytest
from pytest_mock import MockerFixture

from tests.com.code.quality.TestConfig import TestConfig
from src.com.code.quality.AggregationExample import AggregationExample
from unittest.mock import Mock

from pyspark.testing.utils import assertDataFrameEqual


class TestAggregationExample(TestConfig):
    @pytest.fixture
    def aggregation_example(self):
        ob = AggregationExample()
        yield ob

    @pytest.fixture
    def data(self, spark_fixture: SparkSession) -> DataFrame:
        data = [
            {"id": 1, "name": "abc1", "value": 22},
            {"id": 2, "name": "abc1", "value": 23},
            {"id": 3, "name": "def2", "value": 33},
            {"id": 4, "name": "def2", "value": 44},
            {"id": 5, "name": "def2", "value": 55},
        ]
        return spark_fixture.createDataFrame(data)

    def test_sum_val(
        self,
        spark_fixture: SparkSession,
        aggregation_example: AggregationExample,
        data: DataFrame,
    ):
        df_agg = aggregation_example.sum_val(data)
        df_agg.show()

        out = df_agg.collect()
        assert "sum_val" in df_agg.columns
        assert len(out) == 2
        assert out[0]["sum_val"] == 45
        # assert out[0]["sum_val"] == 132

    def test_max_val(
        self,
        spark_fixture: SparkSession,
        aggregation_example: AggregationExample,
        data: DataFrame,
    ):
        df_agg = aggregation_example.max_val(data)

        out = df_agg.collect()
        # out = df_agg.sort("sum_val").collect()
        assert "max_val" in df_agg.columns
        assert len(out) == 2
        assert out[0]["max_val"] == 23
        # assert out[0]["max_val"] == 55

    def test_sort_sum_val(
        self,
        spark_fixture: SparkSession,
        aggregation_example: AggregationExample,
        data: DataFrame,
    ):
        out = aggregation_example.sort_sum_val(data)
        assert len(out) == 2
        assert out[0]["sum_val"] == 132

    def test_sort_sum_val_with_mock(
        self,
        spark_fixture: SparkSession,
        aggregation_example: AggregationExample,
        data: DataFrame,
        mocker: MockerFixture,
    ):
        mocked_data = [
            {"name": "abc1", "sum_val": 45},
            {"name": "def2", "sum_val": 132},
        ]

        # https://dev.to/suvhotta/mocking-using-pytest-210
        mock = Mock()
        mock.return_value = spark_fixture.createDataFrame(mocked_data)

        out_mock = mocker.patch(
            "src.com.code.quality.AggregationExample.AggregationExample.sum_val", mock
        )

        out = aggregation_example.sort_sum_val(data)
        assert out_mock.call_count == 1
        assert len(out) == 2
        assert out[0]["sum_val"] == 132
