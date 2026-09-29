from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts_day").desc_nulls_first())
w_running = Window.partitionBy("attr_key").orderBy(F.col("latency_ms_minus_status_code_rounded").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

cleaned = (
    flattened
    .filter((F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 21:00:00"))) & (F.col("attr_key") == "trace"))
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        "v",
        F.upper("host").alias("host_upper"),
        (F.col("latency_ms") - F.col("status_code")).alias("latency_ms_minus_status_code"),
    )
)

valid = (
    cleaned
    .filter((F.col("log_ts") > F.lit("2025-06-01 21:00:00").cast("timestamp")) & F.col("attr_key").isNotNull() & ~F.col("attr_key").isin(["region", "version"]))
    .withColumn("latency_ms_minus_status_code_whole", F.ceil("latency_ms_minus_status_code"))
)

filtered = (
    valid
    .filter((F.col("log_ts") > F.lit("2025-06-01 20:00:00").cast("timestamp")) & F.col("attr_key").isNotNull())
    .withColumn("log_ts_day", F.dayofmonth("log_ts"))
)

filtered_app_logs = (
    filtered
    .filter((F.col("log_ts") > F.to_timestamp(F.lit("2025-06-02 00:00:00"))) & F.col("attr_key").isNotNull())
    .select(
        "id",
        "host",
        "status_code",
        "attr_key",
        "v",
        "host_upper",
        "latency_ms_minus_status_code_whole",
        "log_ts_day",
        F.round("latency_ms_minus_status_code", 0).alias("latency_ms_minus_status_code_rounded"),
    )
)

result = (
    filtered_app_logs
    .withColumn("log_ts_day_rank", F.rank().over(w))
    .withColumn("latency_ms_minus_status_code_rounded_percent_rank", F.percent_rank().over(w_running))
)
