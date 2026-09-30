from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.partitionBy("attr_key").orderBy("latency_ms")
w3 = Window.partitionBy("attr_key").orderBy(F.col("latency_ms_share").desc())
w4 = Window.orderBy(F.col("latency_ms_share").desc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

enriched = (
    lines
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
)

scored = (
    enriched
    .withColumn("attr_key_avg_latency_ms_share", F.avg("latency_ms_share").over(w))
    .withColumn("corr_status_code_latency_ms", F.corr("status_code", "latency_ms").over(w))
    .withColumn("latency_ms_rank", F.rank().over(w2))
)

cleaned = (
    scored
    .filter(F.col("attr_key").isNotNull())
    .select("*", F.lower("attr_key").alias("attr_key_lower"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.percent_rank().over(w3).alias("latency_ms_share_percent_rank"),
        F.sum("latency_ms_share").over(w).alias("attr_key_sum_latency_ms_share"),
        F.max("attr_key_avg_latency_ms_share").over(w).alias("attr_key_max_attr_key_avg_latency_ms_share"),
        F.percent_rank().over(w4).alias("latency_ms_share_percent_rank_2"),
        F.avg("latency_ms_rank").over(w).alias("attr_key_avg_latency_ms_rank"),
    )
)

matched_keys = (
    metrics
    .select("id")
    .intersectAll(
        df_app_logs
        .filter(F.col("level") == "WARN")
        .select("id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "latency_ms",
        "status_code",
        "attr_key",
        "corr_status_code_latency_ms",
        "latency_ms_rank",
        "latency_ms_share_percent_rank",
        "attr_key_sum_latency_ms_share",
        "latency_ms_share_percent_rank_2",
        "attr_key_avg_latency_ms_rank",
    )
    .orderBy("status_code")
)
