from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "level")
w2 = Window.partitionBy("host").orderBy(F.col("latency_ms").desc_nulls_first())
w3 = Window.partitionBy("k").orderBy(F.col("latency_ms_scaled_filled").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

valid = (
    flattened
    .filter(F.col("k").isNotNull() & (F.col("latency_ms") != 2315))
    .withColumn("level_title", F.initcap(F.trim("level")))
)

filtered = (
    valid
    .filter(F.col("host") != "h4")
    .withColumn("latency_ms_level", F.when(F.col("latency_ms") >= 2632, "High").when(F.col("latency_ms") >= 703, "Medium").otherwise("Low"))
    .withColumn("level_upper", F.upper("level"))
    .withColumn("resolved_k", F.coalesce("k", F.lit("NONE")))
)

valid_app_logs = (
    filtered
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "k",
        "attr_value",
        "level_title",
        "latency_ms_level",
        "level_upper",
        "resolved_k",
        F.round((F.col("latency_ms") * 1.15), 2).alias("latency_ms_scaled"),
        F.upper("host").alias("host_upper"),
        F.coalesce("k", F.lit("N/A")).alias("k_filled"),
    )
)

base = (
    valid_app_logs
    .filter(F.col("k").isNotNull() & F.col("latency_ms_scaled").between(28, 58))
    .select(
        "*",
        F.upper("level").alias("level_upper_2"),
        F.initcap(F.trim("host")).alias("host_title"),
    )
)

base_app_logs = (
    base
    .filter(F.col("latency_ms_scaled") > 78)
    .select(
        "*",
        F.coalesce("latency_ms_scaled", F.lit(0)).alias("latency_ms_scaled_filled"),
        F.trim("level").alias("clean_level"),
        F.coalesce("k", F.lit("NONE")).alias("k_clean"),
    )
)

with_windows = (
    base_app_logs
    .select(
        "*",
        F.min("latency_ms_scaled").over(w).alias("k_min_latency_ms_scaled"),
        F.cume_dist().over(w2).alias("latency_ms_cume_dist"),
        F.dense_rank().over(w3).alias("latency_ms_scaled_filled_dense_rank"),
    )
)

grouped = (
    with_windows
    .groupBy(
        F.when(F.col("latency_ms_scaled_filled_dense_rank") >= 6, "PREMIUM").otherwise("STANDARD").alias("latency_ms_scaled_filled_dense_rank_tier"),
    )
    .agg(
        F.max(F.when(F.col("host") == "h2", F.col("latency_ms_cume_dist"))).alias("h2_max_latency_ms_cume_dist"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("k") == "version", 1)), F.lit(0)), 2).alias("latency_ms_per_version"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
    )
)

result = (
    grouped
    .select(
        "latency_ms_scaled_filled_dense_rank_tier",
        "h2_max_latency_ms_cume_dist",
        "latency_ms_per_version",
        "latency_ms_bit_and",
    )
    .orderBy(F.col("h2_max_latency_ms_cume_dist").asc_nulls_first())
)
