from pyspark.sql import functions as F

lines = (
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

matched_keys = (
    lines
    .select("host", "id")
    .subtract(
        df_app_logs
        .filter(F.col("service") == "checkout")
        .select("host", "id")
    )
)

verified = (
    lines
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("k").isin(["trace", "version"]))
    .withColumn("log_ts_day", F.col("log_ts").cast("date"))
)

filtered_app_logs = (
    filtered
    .filter(F.col("log_ts") > F.lit("2025-06-02").cast("date"))
    .withColumn("latency_ms_num", F.col("latency_ms").cast("double"))
    .withColumn("days_since_log_ts_day", F.datediff(F.current_date(), "log_ts_day"))
)

cleaned = (
    filtered_app_logs
    .filter((F.col("log_ts") >= F.to_date(F.lit("2025-06-01"))) & F.col("k").isNotNull())
    .select(
        "*",
        (F.col("latency_ms_num") + F.col("status_code")).alias("latency_ms_num_plus_status_code"),
        F.hour("log_ts").alias("log_ts_hour"),
    )
)

base = (
    cleaned
    .filter((F.col("log_ts") >= F.to_date(F.lit("2025-06-01"))) & F.col("k").isNotNull())
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "k",
        "value",
        "days_since_log_ts_day",
        "latency_ms_num_plus_status_code",
        F.add_months("log_ts_day", 1).alias("log_ts_day_plus_months"),
        F.when(F.col("latency_ms_num") >= 73, "High").when(F.col("latency_ms_num") >= 29, "Medium").otherwise("Low").alias("latency_ms_num_band"),
    )
)

result = (
    base
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "k",
        "value",
        "days_since_log_ts_day",
        "latency_ms_num_plus_status_code",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
