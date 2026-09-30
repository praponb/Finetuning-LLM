from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "service",
        "latency_ms",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

base = (
    parsed
    .filter(F.col("attr_key").isNotNull())
    .select(
        "*",
        F.length("service").alias("service_length"),
        F.coalesce("attr_key", F.lit("Unknown")).alias("attr_key_filled"),
    )
)

filtered = (
    base
    .filter(F.col("attr_key").isNotNull())
    .withColumn("least_has_fault_status_code_high", F.least("has_fault", "status_code_high"))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

result = (
    filtered
    .cube("service")
    .agg(
        F.sum(F.when(F.col("service") == "checkout", F.col("latency_ms")).otherwise(0)).alias("checkout_latency_ms"),
        F.count(F.when(F.col("attr_key") == "version", 1)).alias("version_rows"),
        F.percentile_approx("service_length", 0.95).alias("service_length_p95"),
    )
    .orderBy(F.col("checkout_latency_ms").desc())
)
