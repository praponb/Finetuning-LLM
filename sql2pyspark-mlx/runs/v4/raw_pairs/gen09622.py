from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.col("context")["region"].alias("region"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.col("context")["version"].alias("version"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    parsed
    .groupBy("level")
    .agg(
        F.max("log_ts").alias("last_seen"),
        F.round(F.avg("is_flagged"), 2).alias("avg_is_flagged"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("level") == "DEBUG", 1)), F.lit(0)), 2).alias("latency_ms_per_debug"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.sum(F.when(F.col("level") == "DEBUG", F.col("is_flagged")).otherwise(0)).alias("debug_is_flagged"),
    )
    .filter(F.col("avg_is_flagged") < 0)
    .orderBy(F.col("level").asc_nulls_first())
)
