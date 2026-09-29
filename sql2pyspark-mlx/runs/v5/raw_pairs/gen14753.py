from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

unpacked = (
    exploded
    .filter(F.col("host").isin(["h4", "h1"]))
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

filtered = (
    unpacked
    .filter(F.col("status_code_high") <= 2)
    .select(
        "*",
        F.substring("host", 1, 4).alias("host_prefix"),
        F.round((F.col("has_fault") * 0.9), 2).alias("adjusted_has_fault"),
    )
)

base = (
    filtered
    .filter(F.col("adjusted_has_fault") >= 43)
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code_high",
        "has_fault",
        "host_prefix",
        (F.col("adjusted_has_fault") * F.col("has_fault")).alias("adjusted_has_fault_times_has_fault"),
    )
)

result = (
    base
    .withColumn("has_fault_tier", F.when(F.col("has_fault") >= 1, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["has_fault_tier", "level", "host"], ["has_fault_tier", "level"], []],
        "has_fault_tier",
        "level",
        "host",
    )
    .agg(
        F.max("status_code_high").alias("max_status_code_high"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_rows"),
        F.sum(F.when(F.col("host") == "h4", F.col("has_fault")).otherwise(0)).alias("h4_has_fault"),
    )
    .filter(F.col("max_status_code_high") > 0)
)
