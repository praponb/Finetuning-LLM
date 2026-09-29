from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

unpacked = (
    lines
    .select(
        "latency_ms",
        "status_code",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

base = (
    unpacked
    .filter(F.col("status_code") >= 271)
    .select(
        "latency_ms",
        "status_code",
        "status_code_high",
        (F.col("latency_ms") / F.nullif("has_fault", F.lit(0))).alias("latency_ms_per_has_fault"),
        (F.col("status_code_high") + F.col("status_code")).alias("status_code_high_plus_status_code"),
    )
)

daily = (
    base
    .groupBy(
        F.when(F.col("status_code_high_plus_status_code") >= 85, "LARGE").otherwise("SMALL").alias("status_code_high_plus_status_code_tier"),
    )
    .agg(
        F.max("status_code_high").alias("max_status_code_high"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.max("latency_ms_per_has_fault").alias("max_latency_ms_per_has_fault"),
    )
)

result = (
    daily
    .select(
        "status_code_high_plus_status_code_tier",
        "avg_latency_ms",
        "latency_ms_bit_or",
        "max_latency_ms_per_has_fault",
    )
    .orderBy("max_latency_ms_per_has_fault", "status_code_high_plus_status_code_tier")
    .limit(10)
)
