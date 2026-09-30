from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("key").orderBy(F.col("latency_ms").asc_nulls_first())
w3 = Window.partitionBy("service", "key")
w4 = Window.partitionBy("key")
w5 = Window.orderBy("latency_ms_share")
w6 = Window.orderBy("status_code")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

enriched = (
    flattened
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
    .withColumn("latency_ms_rank", F.rank().over(w2))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4))
    .withColumn("latency_ms_share_2", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
)

metrics = (
    enriched
    .select(
        "id",
        "status_code",
        "v",
        "latency_ms_rank",
        "latency_ms_share",
        F.round((F.col("latency_ms_rank") / F.sum("latency_ms_rank").over(w4)), 4).alias("latency_ms_rank_share"),
        F.rank().over(w5).alias("latency_ms_share_rank"),
        F.dense_rank().over(w6).alias("status_code_dense_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("latency_ms_share") <= 0.69)
    .select(
        "status_code",
        "v",
        "latency_ms_rank",
        "latency_ms_share",
        "latency_ms_rank_share",
        "latency_ms_share_rank",
        F.greatest("latency_ms_share", "latency_ms_rank").alias("greatest_latency_ms_share_latency_ms_rank"),
    )
)

filtered = (
    valid
    .filter(F.col("greatest_latency_ms_share_latency_ms_rank") >= 34)
    .withColumn("greatest_latency_ms_rank_share_latency_ms_rank", F.greatest("latency_ms_rank_share", "latency_ms_rank"))
)

filtered_app_logs = (
    filtered
    .filter(F.col("latency_ms_share") <= 0.41)
    .withColumn("greatest_latency_ms_share_rank_greatest_latency_ms_rank_share_latency_ms_rank", F.greatest("latency_ms_share_rank", "greatest_latency_ms_rank_share_latency_ms_rank"))
)

result = (
    filtered_app_logs
    .groupBy(
        F.when(F.col("status_code") >= 394, "LARGE").otherwise("SMALL").alias("status_code_tier"),
        F.when(F.col("greatest_latency_ms_rank_share_latency_ms_rank") >= 71, "HIGH").otherwise("LOW").alias("greatest_latency_ms_rank_share_latency_ms_rank_tier"),
    )
    .agg(
        F.sum("latency_ms_share").alias("total_latency_ms_share"),
        F.percentile_approx("latency_ms_share_rank", [0.25, 0.5, 0.75]).alias("latency_ms_share_rank_quartiles"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("num_rows").desc())
)
