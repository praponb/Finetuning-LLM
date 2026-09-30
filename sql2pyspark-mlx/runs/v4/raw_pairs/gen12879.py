from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .withColumn("hour_window", F.date_trunc("HOUR", "log_ts"))
    .withColumn("status_code_tier", F.when(F.col("status_code") >= 397, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["hour_window", "status_code_tier"], ["hour_window"]],
        "hour_window",
        "status_code_tier",
    )
    .agg(
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.min("log_ts").alias("started_at"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.min("log_ts").alias("started_at_2"),
    )
    .orderBy(F.col("span_minutes").desc_nulls_last())
)
