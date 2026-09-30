from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

unpacked = (
    lines
    .filter(F.col("log_ts") >= "2024-05-01")
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "k",
        "attr_value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    unpacked
    .filter(F.col("k").isNotNull() & (F.col("k") != "trace"))
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "k",
        "attr_value",
        "status_code_high",
        F.upper("service").alias("service_upper"),
        F.coalesce("k", F.lit("OTHER")).alias("k_filled"),
    )
)

daily = (
    valid
    .groupBy(
        F.when(F.col("status_code") >= 439, "LARGE").otherwise("SMALL").alias("status_code_tier"),
    )
    .agg(
        F.countDistinct("k").alias("distinct_ks"),
        F.max("status_code").alias("max_status_code"),
        F.percentile_approx("status_code", 0.5).alias("status_code_p50"),
        F.bit_or("status_code").alias("status_code_bit_or"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.round(F.sum("status_code_high") / F.nullif(F.count(F.when(F.col("service") == "billing", 1)), F.lit(0)), 2).alias("status_code_high_per_billing"),
    )
)

result = (
    daily
    .select(
        "status_code_tier",
        "distinct_ks",
        "status_code_p50",
        "status_code_bit_or",
        "span_minutes",
        "status_code_high_per_billing",
    )
)
