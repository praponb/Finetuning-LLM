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
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("status_code") >= 385, "LARGE").otherwise("SMALL").alias("status_code_tier"),
    )
    .agg(
        F.stddev_samp("status_code").alias("status_code_stddev"),
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_rows"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("level") == "INFO", 1)), F.lit(0)), 2).alias("latency_ms_per_info"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("status_code_stddev") > 0)
)
