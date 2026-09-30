from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

unpacked = (
    lines
    .filter(F.col("service") == "search")
    .select(
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "key",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)

valid = (
    unpacked
    .filter((F.col("log_ts") > F.lit("2025-06-01 12:00:00").cast("timestamp")) & (~F.col("key").eqNullSafe(F.lit("trace"))))
    .withColumn("host_upper", F.upper("host"))
    .withColumn("status_code_num", F.col("status_code").cast("string"))
)

filtered = (
    valid
    .filter(F.col("log_ts") < "2023-09-01")
    .withColumn("service_upper", F.upper("service"))
)

cleaned = (
    filtered
    .filter(F.col("key").isNotNull())
    .select(
        "service",
        "host",
        "latency_ms",
        "status_code",
        "value",
        "status_code_high",
        "flag_bit1",
        "host_upper",
        "status_code_num",
        "service_upper",
        (F.col("status_code") / F.nullif("status_code_high", F.lit(0))).alias("status_code_per_status_code_high"),
    )
)

result = (
    cleaned
    .groupBy(
        "service",
        F.when(F.col("flag_bit1") >= 1, "HIGH").otherwise("LOW").alias("flag_bit1_tier"),
    )
    .agg(
        F.count_if(F.col("service") == "search").alias("search_events"),
        F.count("*").alias("row_count"),
        F.corr("status_code", "latency_ms").alias("corr_status_code_latency_ms"),
        F.percentile_approx("status_code_high", 0.5).alias("status_code_high_p50"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
    )
    .orderBy(F.col("row_count").desc())
)
