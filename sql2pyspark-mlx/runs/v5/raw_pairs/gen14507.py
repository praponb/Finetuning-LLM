from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "k")
w2 = Window.orderBy(F.col("latency_ms").desc_nulls_first())
w3 = Window.partitionBy("k", "service")
w4 = Window.partitionBy("service").orderBy("latency_ms")
w_ordered = Window.partitionBy("service").orderBy(F.col("latency_ms").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        F.explode_outer("context").alias("k", "value"),
    )
)

remaining_keys = (
    lines
    .select("service")
    .intersect(
        df_app_logs
        .filter(F.col("host") != "h1")
        .select("service")
    )
)

reconciled = (
    lines
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("k").isNotNull() & (~F.col("k").eqNullSafe(F.lit("region"))))
    .select(
        "*",
        F.when(F.col("k").isin(["trace", "version"]), "Tier 1").otherwise("Other").alias("k_group"),
    )
)

metrics = (
    filtered
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("latency_ms_rank", F.rank().over(w2))
    .withColumn("k_stddev_samp_latency_ms", F.stddev_samp("latency_ms").over(w3))
    .withColumn("latency_ms_rank_2", F.rank().over(w4))
    .withColumn("latency_ms_rank_3", F.rank().over(w_ordered))
)

base = (
    metrics
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "service",
        "latency_ms",
        "k",
        "value",
        "k_group",
        "latency_ms_share",
        "latency_ms_rank",
        "k_stddev_samp_latency_ms",
        "latency_ms_rank_2",
        F.round("latency_ms_rank_2", 1).alias("latency_ms_rank_2_rounded"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_filled"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("k_stddev_samp_latency_ms") >= 1930, "HIGH").otherwise("LOW").alias("k_stddev_samp_latency_ms_tier"),
        "k",
    )
    .agg(
        F.count_if(F.col("service") == "search").alias("search_events"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("latency_ms_rank_2_rounded"), 2).alias("avg_latency_ms_rank_2_rounded"),
        F.count_if(F.col("k") == "version").alias("version_events"),
    )
    .orderBy(F.col("k_stddev_samp_latency_ms_tier").desc_nulls_last())
)
