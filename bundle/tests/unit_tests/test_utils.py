from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from Windturbineproject.bundle.src.utils import cleanse_data, calculate_turbine_summary, identify_anomalies

def test_cleanse_data():
    spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
    data = [
        (1, "A", 10.0, None),
        (2, None, None, 20),
        (3, "B", 30.0, 30),
        (3, "B", 30.0, 30),  # Duplicate row
    ]
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("value", FloatType(), True),
        StructField("integer_field", IntegerType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    cleaned_df = cleanse_data(df)

    # Check duplicates are removed
    assert cleaned_df.count() == 3

    # Check missing categorical values are filled with "unknown"
    assert cleaned_df.filter(cleaned_df.id == 2).select("category").first()[0] == "unknown"

    # Check rows with null integer fields are filtered out
    assert cleaned_df.filter(cleaned_df.integer_field.isNull()).count() == 0

    spark.stop()

def test_calculate_turbine_summary():
    spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
    data = [
        ("2023-01-01 10:00:00", "T1", 50.0),
        ("2023-01-01 11:00:00", "T1", 60.0),
        ("2023-01-01 10:00:00", "T2", 70.0),
    ]
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("turbine_id", StringType(), True),
        StructField("power_output", FloatType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    summary_df = calculate_turbine_summary(df)

    # Check average power calculation
    assert summary_df.filter(summary_df.turbine_id == "T1").select("avg_power").first()[0] == 55.0

    spark.stop()

def test_identify_anomalies():
    spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
    data = [
        ("T1", "2023-01-01", 50.0),
        ("T2", "2023-01-01", 100.0),
        ("T3", "2023-01-01", 200.0),
    ]
    schema = StructType([
        StructField("turbine_id", StringType(), True),
        StructField("date", StringType(), True),
        StructField("avg_power", FloatType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    anomaly_df = identify_anomalies(df, threshold=1)

    # Check if anomalies are identified correctly
    assert anomaly_df.filter(anomaly_df.is_anomaly == True).count() == 2

    spark.stop()
