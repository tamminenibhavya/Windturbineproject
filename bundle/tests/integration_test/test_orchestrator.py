import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from extract_data import extract_turbine_data
from transform_data import create_turbine_cleansed_data_silver, create_turbine_data_gold
from load_data import load_to_gold


# Integration Test
def test_pipeline_integration():
    # Mock SparkSession

    with patch.object(SparkSession, 'builder') as mock_builder:
        mock_spark = mock_builder.getOrCreate.return_value

        # Mock configurations
        mock_spark.conf.get.side_effect = lambda key: {
            "catalog_name": "test_catalog",
            "schema_name": "test_schema",
            "volume_name": "test_volume"
        }.get(key, "")

        # Mock volume path
        volume_path = f"/Volumes/test_catalog/test_schema/test_volume/"

        # Mock functions

        with patch('extract_data.extract_turbine_data') as mock_extract, \
             patch('transform_data.create_turbine_cleansed_data_silver') as mock_transform_silver, \
             patch('transform_data.create_turbine_data_gold') as mock_transform_gold, \
             patch('load_data.load_to_gold') as mock_load:

            # Run the pipeline
            extract_turbine_data(mock_spark, volume_path)
            create_turbine_cleansed_data_silver(mock_spark)
            create_turbine_data_gold(mock_spark)
            load_to_gold(mock_spark)

            # Assertions
            mock_extract.assert_called_once_with(mock_spark, volume_path)
            mock_transform_silver.assert_called_once_with(mock_spark)
            mock_transform_gold.assert_called_once_with(mock_spark)
            mock_load.assert_called_once_with(mock_spark)

# Call the test function
test_pipeline_integration()