# Databricks notebook source

import dlt
import logging

from extract_data import extract_turbine_data
from pyspark.sql import SparkSession
from transform_data import create_turbine_cleansed_data_silver, create_turbine_data_gold
from load_data import load_to_gold

spark = SparkSession.builder.getOrCreate()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

catalog = spark.conf.get("catalog_name") # type: ignore
schema = spark.conf.get("schema_name") # type: ignore
volume_name = spark.conf.get("volume_name") # # type: ignore

# Define the path to source data
volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}/"

# Extract
logging.info(f"Starting extraction from {volume_path}")
try:
    extract_turbine_data(spark, volume_path)
    logging.info("Data extraction completed successfully.")
except Exception as e:
    logging.error(f"Error during data extraction: {e}")
    

# Transform
try:
    logging.info("Starting transformation to create cleansed data.")
    create_turbine_cleansed_data_silver(spark)
    logging.info("Cleansed data transformation completed successfully.")
    
    logging.info("Starting transformation to create gold data.")
    create_turbine_data_gold(spark)
    logging.info("Gold data transformation completed successfully.")
except Exception as e:
    logging.error(f"Error during data transformation: {e}")

# Load
try:
    logging.info("Starting loading data to gold.")
    load_to_gold(spark)
    logging.info("Data loading completed successfully.")
except Exception as e:
    logging.error(f"Error during data loading: {e}")
