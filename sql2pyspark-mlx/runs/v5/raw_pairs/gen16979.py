from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("service").orderBy(F.col("status_code_share").desc())
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy("service")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("k").isNull())
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.lower("service").alias("service_lower"),
        F.when(F.col("level") == "WARN", 1).otherwise(0).alias("is_warn"),
    )
)

scored = (
    cleaned
    .filter(F.col("service") == "billing")
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "is_warn",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "service",
        "log_ts",
        "status_code_share",
        F.cume_dist().over(w2).alias("status_code_share_cume_dist"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.count("status_code_share_cume_dist").over(w3).alias("status_code_share_cume_dist_count_15m"),
        F.avg("status_code_share").over(w4).alias("service_avg_status_code_share"),
    )
)

stats = (
    enriched
    .groupBy(
        F.when(F.col("status_code_share_cume_dist") >= 0.85, "PREMIUM").otherwise("STANDARD").alias("status_code_share_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("status_code_share", 0.95).alias("status_code_share_p95"),
        F.min("log_ts").alias("started_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
        F.max("log_ts").alias("last_log_ts"),
    )
)

result = (
    stats
    .select(
        "status_code_share_cume_dist_tier",
        "status_code_share_p95",
        "distinct_ids",
        "auth_rows",
        "last_log_ts",
    )
    .orderBy(F.col("last_log_ts").desc())
)
