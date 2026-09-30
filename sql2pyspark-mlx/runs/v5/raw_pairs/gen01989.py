from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.partitionBy("level", "k")
w3 = Window.orderBy(F.col("latency_ms").desc_nulls_first())
w4 = Window.orderBy("latency_ms")

exploded = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

metrics = (
    exploded
    .filter(F.col("k") == "trace")
    .select(
        "id",
        "k",
        "attr_value",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share_2"),
        F.count("latency_ms").over(w2).alias("level_count_latency_ms"),
        F.rank().over(w3).alias("latency_ms_rank"),
        F.cume_dist().over(w4).alias("latency_ms_cume_dist"),
    )
)

cleaned = (
    metrics
    .filter(~F.col("k").isin(["version", "region"]))
    .select(
        "*",
        F.lower("k").alias("k_lower"),
        F.when(F.col("k") == "region", 1).otherwise(0).alias("is_region"),
        F.coalesce("k", F.lit("N/A")).alias("resolved_k"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("latency_ms_share").between(0.31, 0.76))
    .select(
        "id",
        "attr_value",
        "latency_ms_share",
        "latency_ms_share_2",
        "level_count_latency_ms",
        "latency_ms_rank",
        "latency_ms_cume_dist",
        "k_lower",
        (F.col("latency_ms_rank") / F.nullif("is_region", F.lit(0))).alias("latency_ms_rank_per_is_region"),
        F.upper("k").alias("k_upper"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_filled"),
    )
)

filtered = (
    cleaned_app_logs
    .filter(F.col("latency_ms_rank_per_is_region") < 63)
    .select(
        "id",
        "attr_value",
        "latency_ms_share",
        "level_count_latency_ms",
        "latency_ms_rank",
        "k_lower",
        "latency_ms_rank_per_is_region",
        "k_filled",
        (F.col("level_count_latency_ms") * F.col("latency_ms_cume_dist")).alias("level_count_latency_ms_times_latency_ms_cume_dist"),
        F.round((F.col("level_count_latency_ms") * 1.1), 2).alias("adjusted_level_count_latency_ms"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("level_count_latency_ms") >= 18, "HIGH").otherwise("LOW").alias("level_count_latency_ms_tier"),
    )
    .agg(
        F.percentile_approx("level_count_latency_ms", [0.25, 0.5, 0.75]).alias("level_count_latency_ms_quartiles"),
        F.percentile_approx("level_count_latency_ms", 0.5).alias("level_count_latency_ms_p50"),
    )
)
