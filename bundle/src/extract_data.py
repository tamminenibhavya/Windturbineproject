import dlt
from schemas import file_schema
from pyspark.sql import functions as f


def extract_turbine_data(spark, volume_path):
    @dlt.table(
        name = "turbine_data_bronze",
        comment = "Load raw turbine data into bronze table"
    )
    def raw_turbine_bronze():
        df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("header", "true") \
        .option("inferschema", "true") \
        .option("cloudFiles.schemaLocation", volume_path+"/bronze/checkpoint")\
        .schema(file_schema)\
        .load(volume_path)\
        .withColumn("file_path", f.col("_metadata.file_path"))\
        .withColumn("load_timestamp", f.current_timestamp())
        return df 