from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

flattened_attrs = (
    lines
    .select(
        "id",
        "log_ts",
        "level",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
    )
)

cleaned = (
    flattened_attrs
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select(
        "id",
        "log_ts",
        "k",
        "status_code_high",
        "has_fault",
        F.round("status_code_high", 0).alias("status_code_high_rounded"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .select(
        "id",
        "log_ts",
        "status_code_high",
        "has_fault",
        F.initcap(F.trim("k")).alias("k_title"),
    )
)

base = (
    cleaned_app_logs
    .filter((F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 10:00:00"))) & (F.to_date("log_ts") < "2024-11-01"))
    .select(
        "id",
        "log_ts",
        "status_code_high",
        "has_fault",
        "k_title",
        F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"),
    )
)

filtered = (
    base
    .filter(F.col("status_code_high") != 1)
    .select(
        "id",
        "log_ts",
        "status_code_high",
        "has_fault",
        "k_title",
        "days_since_log_ts",
        (F.col("days_since_log_ts") / F.nullif("status_code_high", F.lit(0))).alias("days_since_log_ts_per_status_code_high"),
        F.greatest("status_code_high", "days_since_log_ts").alias("greatest_status_code_high_days_since_log_ts"),
    )
)

stats = (
    filtered
    .groupBy(F.date_trunc("hour", "log_ts").alias("hour_window"))
    .agg(
        F.max("log_ts").alias("ended_at"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") > 1)
)

result = (
    stats
    .select("hour_window", "ended_at", "distinct_ids")
    .orderBy(F.col("hour_window").desc_nulls_last())
)
