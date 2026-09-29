from pyspark.sql import functions as F

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
        F.explode_outer("context").alias("key", "value"),
    )
)

unpacked = (
    flattened
    .select(
        "service",
        "level",
        "latency_ms",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

valid = (
    unpacked
    .filter(F.col("has_fault") == 0)
    .select(
        "service",
        "latency_ms",
        "value",
        "status_code_high",
        "has_fault",
        F.greatest("status_code_high", "has_fault").alias("greatest_status_code_high_has_fault"),
    )
)

valid_app_logs = (
    valid
    .filter(F.col("status_code_high") == 2)
    .withColumn("latency_ms_plus_status_code_high", (F.col("latency_ms") + F.col("status_code_high")))
)

result = (
    valid_app_logs
    .select("service", "value", "latency_ms_plus_status_code_high")
    .orderBy(F.col("latency_ms_plus_status_code_high").desc())
)
