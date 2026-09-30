from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "k",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

kept_keys = (
    parsed
    .select("service", "id")
    .exceptAll(
        df_app_logs
        .filter(F.col("status_code") <= 362)
        .select("service", "id")
    )
)

reconciled = (
    parsed
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

grouped = (
    reconciled
    .cube(
        "k",
        F.when(F.col("status_code") >= 435, "LARGE").otherwise("SMALL").alias("status_code_tier"),
        F.when(F.col("latency_ms") >= 2568, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
    )
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.percentile_approx("latency_ms", 0.9).alias("latency_ms_p90"),
        F.sum("status_code_high").alias("total_status_code_high"),
        F.count(F.when(F.col("k") == "version", 1)).alias("version_rows"),
    )
    .filter(F.col("total_status_code_high") >= 0)
)

result = (
    grouped
    .select("k", "status_code_tier", "latency_ms_p90", "total_status_code_high")
    .orderBy(F.col("total_status_code_high").desc_nulls_first())
)
