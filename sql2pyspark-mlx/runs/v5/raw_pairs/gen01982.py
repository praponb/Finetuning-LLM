from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    lines
    .groupBy("level", F.date_trunc("HOUR", "log_ts").alias("hour_window"), "host")
    .agg(
        F.corr("status_code", "latency_ms").alias("corr_status_code_latency_ms"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("hour_window").desc())
)
