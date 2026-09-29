from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    flattened
    .cube(
        F.date_trunc("day", "log_ts").alias("activity_day"),
        F.when(F.col("latency_ms") >= 2432, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_rows"),
        F.min("log_ts").alias("first_log_ts"),
        F.max("latency_ms").alias("max_latency_ms"),
    )
    .orderBy(F.col("latency_ms_tier").desc_nulls_first())
)
