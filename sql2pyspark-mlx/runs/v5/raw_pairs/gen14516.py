from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "context",
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        F.when(F.col("status_code") >= 455, "LARGE").otherwise("SMALL").alias("status_code_tier"),
        "service",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.corr("status_code", "latency_ms").alias("corr_status_code_latency_ms"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("service") == "checkout", 1)), F.lit(0)), 2).alias("latency_ms_per_checkout"),
    )
    .orderBy(F.col("latency_ms_bit_and").desc())
)
