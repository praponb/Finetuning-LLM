from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

parsed = (
    exploded
    .filter(~F.col("service").isin(["checkout", "billing", "search"]))
    .select(
        "service",
        "status_code",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

filtered = (
    parsed
    .filter(F.col("attr_key").isNotNull())
    .select("*", F.coalesce("attr_key", F.lit("Unknown")).alias("attr_key_filled"))
)

valid = (
    filtered
    .filter(F.col("attr_key").isNotNull())
    .select(
        "*",
        F.col("status_code_high").cast("double").alias("status_code_high_num"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("resolved_attr_key"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("status_code_high") >= 2, "LARGE").otherwise("SMALL").alias("status_code_high_tier"),
        "attr_key",
        "service",
    )
    .agg(
        F.max("status_code").alias("max_status_code"),
        F.min("has_fault").alias("min_has_fault"),
        F.percentile_approx("status_code_high", 0.9).alias("status_code_high_p90"),
        F.avg(F.when(F.col("attr_key") == "trace", F.col("has_fault"))).alias("trace_avg_has_fault"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_count"),
        F.max("status_code").alias("max_status_code_2"),
    )
)
