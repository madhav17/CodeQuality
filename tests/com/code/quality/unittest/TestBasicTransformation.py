from pyspark.testing import assertDataFrameEqual

from src.com.code.quality.BasicTransformation import BasicTransformation


from com.code.quality.unittest.PySparkTestCase import PySparkTestCase


class TestBasicTransformation(PySparkTestCase):
    def setUp(self):
        super().setUp()
        self.__ob = BasicTransformation

    def test_space_removal(self):
        sample_data = [
            {"name": "John    D.", "age": 30},
            {"name": "Alice   G.", "age": 25},
            {"name": "Bob  T.", "age": 35},
            {"name": "Eve   A.", "age": 28},
        ]

        original_df = self.spark.createDataFrame(sample_data)

        # Apply the transformation function from before
        transformed_df = self.__ob.remove_extra_spaces(
            self=self.__ob, df=original_df, column_name="name"
        )

        expected_data = [
            {"name": "John D.", "age": 30},
            {"name": "Alice G.", "age": 25},
            {"name": "Bob T.", "age": 35},
            {"name": "Eve A.", "age": 28},
        ]

        expected_df = self.spark.createDataFrame(expected_data)

        assertDataFrameEqual(transformed_df, expected_df)
