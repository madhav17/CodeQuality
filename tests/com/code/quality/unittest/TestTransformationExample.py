from pyspark.testing import assertDataFrameEqual

from com.code.quality.TransformationExample import TransformationExample

from com.code.quality.unittest.PySparkTestCase import PySparkTestCase


class TestTransformationExample(PySparkTestCase):
    def setUp(self):
        super().setUp()
        self.__ob = TransformationExample

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
            self=self.__ob, data_frame=original_df, column_name="name"
        )

        expected_data = [
            {"name": "John D.", "age": 30},
            {"name": "Alice G.", "age": 25},
            {"name": "Bob T.", "age": 35},
            {"name": "Eve A.", "age": 28},
        ]

        expected_df = self.spark.createDataFrame(expected_data)

        assertDataFrameEqual(transformed_df, expected_df)
