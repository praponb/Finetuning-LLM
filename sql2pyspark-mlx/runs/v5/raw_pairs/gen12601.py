from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").desc_nulls_last())
w2 = Window.orderBy(F.col("latency_ms").desc_nulls_first())
w3 = Window.partitionBy("key")
w4 = Window.orderBy(F.col("status_code").desc())
w5 = Window.partitionBy("key").orderBy(F.col("latency_ms_dense_rank").desc_nulls_last())
w6 = Window.partitionBy("key").orderBy(F.col("is_flagged").desc_nulls_first())
w_recent = Window.partitionBy("host").orderBy(F.col("status_code_high").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

flattened_attrs = (
    flattened
    .filter(F.col("host") != "h4")
    .select(
        "host",
        "latency_ms",
        "status_code",
        "key",
        "v",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    flattened_attrs
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w))
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w2))
    .withColumn("key_avg_is_flagged", F.avg("is_flagged").over(w3))
    .withColumn("status_code_percent_rank", F.percent_rank().over(w4))
)

enriched = (
    with_windows
    .select(
        "*",
        F.percent_rank().over(w5).alias("latency_ms_dense_rank_percent_rank"),
        F.percent_rank().over(w6).alias("is_flagged_percent_rank"),
        F.cume_dist().over(w_recent).alias("status_code_high_cume_dist"),
    )
)

valid = (
    enriched
    .filter(F.col("key").isNotNull())
    .select(
        "host",
        "latency_ms",
        "status_code",
        "key",
        "v",
        "is_flagged",
        "status_code_high",
        "latency_ms_dense_rank",
        "latency_ms_percent_rank",
        "status_code_percent_rank",
        "latency_ms_dense_rank_percent_rank",
        "is_flagged_percent_rank",
        "status_code_high_cume_dist",
        F.col("is_flagged_percent_rank").cast("int").alias("is_flagged_percent_rank_int"),
        (F.col("latency_ms_dense_rank_percent_rank") + F.col("status_code_high_cume_dist")).alias("latency_ms_dense_rank_percent_rank_plus_status_code_high_cume_dist"),
    )
)

valid_app_logs = (
    valid
    .filter(F.col("key").isNotNull())
    .withColumn("is_flagged_percent_rank_int_whole", F.ceil("is_flagged_percent_rank_int"))
    .withColumn("latency_ms_dense_rank_percent_rank_plus_status_code_high_cume_dist_per_status_code_high_cume_dist", (F.col("latency_ms_dense_rank_percent_rank_plus_status_code_high_cume_dist") / F.nullif("status_code_high_cume_dist", F.lit(0))))
    .withColumn("key_clean", F.coalesce("key", F.lit("N/A")))
)

result = (
    valid_app_logs
    .select(
        "key",
        "is_flagged",
        "status_code_high",
        "latency_ms_dense_rank",
        "status_code_percent_rank",
        "is_flagged_percent_rank",
        "status_code_high_cume_dist",
        "is_flagged_percent_rank_int",
        "latency_ms_dense_rank_percent_rank_plus_status_code_high_cume_dist",
    )
    .orderBy(
        F.col("latency_ms_dense_rank_percent_rank_plus_status_code_high_cume_dist").desc(),
    )
)
