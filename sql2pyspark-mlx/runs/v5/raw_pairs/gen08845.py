from pyspark.sql import functions as F

unpivoted = (
    df_device_readings
    .select(
        "id",
        "device_code",
        F.col("temp_c").alias("TEMP_C_VAL"),
        F.col("humidity").alias("HUMIDITY"),
        "pressure",
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["TEMP_C_VAL", "HUMIDITY", "pressure"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)

result = (
    unpivoted
    .groupBy("measure", "device_code")
    .agg(
        F.count("*").alias("cnt"),
        F.min("metric_value").alias("min_metric_value"),
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("metric_value")).otherwise(0)).alias("dv_04_metric_value"),
        F.round(F.avg("metric_value"), 2).alias("avg_metric_value"),
    )
    .filter(F.col("min_metric_value") >= 0)
    .orderBy(F.col("measure").desc())
)
