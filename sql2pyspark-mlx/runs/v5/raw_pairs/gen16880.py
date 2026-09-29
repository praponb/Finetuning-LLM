from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code_per_latency_ms")
w_part = Window.orderBy(F.col("status_code_per_latency_ms_cume_dist").desc())
w3 = Window.orderBy(F.col("status_code_per_latency_ms_cume_dist").desc_nulls_last())
w4 = Window.orderBy(F.col("status_code_num").asc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

base = (
    exploded
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("key").isNotNull())
    .withColumn("key_upper", F.upper("key"))
)

cleaned = (
    base
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & (F.col("key") == "trace"))
    .select(
        "log_ts",
        "v",
        "key_upper",
        F.round((F.col("status_code") / F.col("latency_ms")), 2).alias("status_code_per_latency_ms"),
        F.col("status_code").cast("double").alias("status_code_num"),
        F.coalesce("key", F.lit("N/A")).alias("resolved_key"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("status_code_num") <= 36)
    .select(
        "v",
        "status_code_per_latency_ms",
        "status_code_num",
        "resolved_key",
        F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"),
    )
)

metrics = (
    cleaned_app_logs
    .select(
        "v",
        "status_code_num",
        "resolved_key",
        F.cume_dist().over(w).alias("status_code_per_latency_ms_cume_dist"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.rank().over(w_part).alias("status_code_per_latency_ms_cume_dist_rank"),
    )
)

scored = (
    enriched
    .withColumn("status_code_per_latency_ms_cume_dist_cume_dist", F.cume_dist().over(w3))
    .withColumn("status_code_num_percent_rank", F.percent_rank().over(w4))
)

result = (
    scored
    .groupBy(
        F.when(F.col("status_code_num") >= 87, "HIGH").otherwise("LOW").alias("status_code_num_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("status_code_per_latency_ms_cume_dist_rank", 0.95).alias("status_code_per_latency_ms_cume_dist_rank_p95"),
        F.percentile_approx("status_code_per_latency_ms_cume_dist_rank", [0.25, 0.5, 0.75]).alias("status_code_per_latency_ms_cume_dist_rank_quartiles"),
        F.sum("status_code_per_latency_ms_cume_dist_rank").alias("total_status_code_per_latency_ms_cume_dist_rank"),
    )
    .orderBy(F.col("total_status_code_per_latency_ms_cume_dist_rank").asc_nulls_first())
)
