"""
Unit tests for the GenAI PySpark Pipeline
"""

import unittest
from src.pipeline import GenAIPySparkPipeline

class TestGenAIPySparkPipeline(unittest.TestCase):

    def setUp(self):
        self.pipeline = GenAIPySparkPipeline()

    def test_init_spark(self):
        self.pipeline.init_spark()
        self.assertIsNotNone(self.pipeline.spark)
        self.assertEqual(self.pipeline.spark.sparkContext.appName, "GenAI PySpark Pipeline")

    def test_generate_synthetic_data(self):
        self.pipeline.init_spark()
        df = self.pipeline.generate_synthetic_data(10)
        self.assertEqual(df.count(), 10)
        columns = df.columns
        expected_columns = ['name', 'email', 'address', 'phone', 'company', 'job', 'age', 'salary']
        self.assertEqual(columns, expected_columns)

if __name__ == '__main__':
    unittest.main()