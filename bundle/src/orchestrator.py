# Databricks notebook source

import dlt

from extract_data import extract_turbine_data
from pyspark.sql import SparkSession
from transform_data import create_turbine_cleansed_data_silver, create_turbine_data_gold
from load_data import load_to_gold

spark = SparkSession.builder.getOrCreate()

catalog = spark.conf.get("catalog_name") # type: ignore
schema = spark.conf.get("schema_name") # type: ignore
volume_name = spark.conf.get("volume_name") # # type: ignore

# Define the path to source data
volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}/"

# Extract
extract_turbine_data(spark, volume_path)

# Transform
create_turbine_cleansed_data_silver(spark)
create_turbine_data_gold(spark)

# Load
load_to_gold(spark)