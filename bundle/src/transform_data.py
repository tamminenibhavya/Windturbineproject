import dlt
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import Imputer
from utils import calculate_turbine_summary, identify_anomalies, cleanse_data

valid_cleansed_data = {
    "Timestamp Present": "timestamp IS NOT NULL",
    "Turbine ID Valid": "turbine_id IS NOT NULL AND turbine_id > 0",
    "Wind Speed Present": "wind_speed IS NOT NULL",
    "Wind Direction Present": "wind_direction IS NOT NULL",
    "Power Output Valid": "power_output >= 0 AND power_output IS NOT NULL"
}

def create_turbine_cleansed_data_silver(spark):
    @dlt.table(
        name="turbine_data_silver",
        comment="Create table by cleansing the raw data",
        table_properties={
            "pipelines.autoOptimize.managed": "true"
            }
        )
    @dlt.expect_all_or_drop(valid_cleansed_data)
    def turbine_data_silver():
        df = dlt.read("raw_turbine_data")
        return cleanse_data(df)
    




def create_turbine_data_gold(spark):
    @dlt.view(
        name = "turbine_data_gold_vw",
        comment="Turbine data cleansed and ready for analysis"
    )
    def turbine_data_gold():
        # Read the cleansed view
        df = dlt.read("turbine_data_silver")

        # Apply summary and anomaly detection
        summary_df = calculate_turbine_summary(df)

        anomalies_df = identify_anomalies(summary_df)

        return anomalies_df