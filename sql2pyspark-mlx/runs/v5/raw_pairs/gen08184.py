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
        F.explode_outer("context").alias("key", "value"),
    )
)

flattened_attrs = (
    lines
    .filter(F.col("latency_ms") != 2250)
    .select(
        "latency_ms",
        "status_code",
        "value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

filtered = (
    flattened_attrs
    .filter(F.col("status_code").between(314, 382))
    .withColumn("latency_ms_filled", F.coalesce("latency_ms", F.lit(0)))
)

base = (
    filtered
    .filter(F.col("status_code_high") != 2)
    .select(
        "latency_ms",
        "status_code",
        "has_fault",
        "status_code_high",
        F.round((F.col("has_fault") * 0.9), 2).alias("has_fault_adjusted"),
    )
)

result = (
    base
    .select("status_code", "status_code_high", "has_fault_adjusted")
    .orderBy(F.col("status_code_high").desc(), "has_fault_adjusted")
)
