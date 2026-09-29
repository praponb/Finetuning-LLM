from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("device_code")
    .agg(
        F.avg("pressure").alias("avg_pressure"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("reading_ts").alias("first_seen"),
    )
    .orderBy("device_code")
)
