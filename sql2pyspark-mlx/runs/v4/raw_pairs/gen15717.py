from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.partitionBy("k").orderBy("adjusted_status_code")
w3 = Window.partitionBy("k").orderBy(F.col("adjusted_status_code").desc_nulls_last())
w4 = Window.partitionBy("k").orderBy(F.col("num_ks").desc())

flattened = (
    df_app_logs
    .select("id", "status_code", F.explode_outer("context").alias("k", "value"))
)

valid = (
    flattened
    .filter(F.col("k").isNotNull())
    .select("*", F.round((F.col("status_code") * 1.1), 2).alias("adjusted_status_code"))
)

scored = (
    valid
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
    .withColumn("adjusted_status_code_rank", F.rank().over(w2))
    .withColumn("status_code_share_2", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
)

metrics = (
    scored
    .withColumn("adjusted_status_code_cume_dist", F.cume_dist().over(w3))
    .withColumn("k_count_adjusted_status_code", F.count("adjusted_status_code").over(w))
    .withColumn("k_min_adjusted_status_code_rank", F.min("adjusted_status_code_rank").over(w))
    .withColumn("adjusted_status_code_rank_share", F.round((F.col("adjusted_status_code_rank") / F.sum("adjusted_status_code_rank").over(w)), 4))
)

with_windows = (
    metrics
    .withColumn("k_avg_adjusted_status_code_rank", F.avg("adjusted_status_code_rank").over(w))
    .withColumn("k_sum_adjusted_status_code_rank_share", F.sum("adjusted_status_code_rank_share").over(w))
)

summary = (
    with_windows
    .groupBy(
        "k",
        F.when(F.col("adjusted_status_code") >= 72, "HIGH").otherwise("LOW").alias("adjusted_status_code_tier"),
    )
    .agg(
        F.avg(F.when(F.col("k") == "region", F.col("status_code"))).alias("region_avg_status_code"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.size(F.collect_set("k")).alias("num_ks_2"),
    )
    .withColumn("num_ks_percent_rank", F.percent_rank().over(w4))
)

result = (
    summary
    .select(
        "k",
        "adjusted_status_code_tier",
        "region_avg_status_code",
        "num_ks",
        "num_ks_2",
        "num_ks_percent_rank",
    )
)
