from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("status_code") >= 459, "HIGH").otherwise("LOW").alias("status_code_tier"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        "host",
    )
    .agg(
        F.count_if(F.col("k") == "trace").alias("trace_events"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("host") == "h2", 1)), F.lit(0)), 2).alias("status_code_per_h2"),
    )
    .orderBy(F.col("trace_events").asc_nulls_last())
)
