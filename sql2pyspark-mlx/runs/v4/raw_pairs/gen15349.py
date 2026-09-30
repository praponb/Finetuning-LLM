from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

flattened_attrs = (
    exploded
    .filter(F.col("latency_ms") == 1597)
    .select(
        "id",
        "latency_ms",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
    )
)

valid = (
    flattened_attrs
    .filter(F.col("status_code_high") != 2)
    .withColumn("status_code_high_tier", F.when(F.col("status_code_high") >= 3, "High").when(F.col("status_code_high") >= 1, "Medium").otherwise("Low"))
    .withColumn("is_flagged_str", F.col("is_flagged").cast("string"))
)

grouped = (
    valid
    .groupBy(
        F.when(F.col("latency_ms") >= 1957, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
        F.when(F.col("status_code_high") >= 2, "PREMIUM").otherwise("STANDARD").alias("status_code_high_tier"),
    )
    .agg(
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.percentile_approx("status_code_high", 0.95).alias("status_code_high_p95"),
        F.max("latency_ms").alias("max_latency_ms"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and_2"),
    )
)

result = (
    grouped
    .select(
        "latency_ms_tier",
        "status_code_high_tier",
        "latency_ms_quartiles",
        "max_latency_ms",
        "latency_ms_bit_and_2",
    )
    .orderBy(F.col("latency_ms_tier").desc_nulls_first(), "status_code_high_tier")
)
