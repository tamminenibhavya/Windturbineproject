import dlt
from pyspark.sql.functions import avg, max, min, current_timestamp, lit

def load_to_gold(spark):
    @dlt.table(
        comment="A table summarizing statistics by day for each turbine"
    )
    def turbine_data_gold():
        df = spark.read.table("turbine_data_gold_vw")
        return (
            df.withColumn("created_at", current_timestamp())
              .withColumn("created_by", lit("Lakeflow Pipeline"))
        )