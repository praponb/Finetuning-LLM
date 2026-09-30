from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    exploded
    .groupBy("k")
    .agg(
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.avg("status_code").alias("avg_status_code"),
        F.countDistinct("k").alias("distinct_ks"),
    )
    .filter(F.col("distinct_ks") > 1)
)
