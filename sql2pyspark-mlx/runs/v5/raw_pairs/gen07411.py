from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("val")
w2 = Window.orderBy(F.col("val").asc_nulls_first())
w_ordered = Window.partitionBy("plan")
w4 = Window.orderBy(F.col("val_percent_rank").desc())
w_running = Window.orderBy(F.col("val_percent_rank").desc_nulls_first())
w6 = Window.partitionBy("plan").orderBy(F.col("val_percent_rank").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("val"),
    )
)

metrics = (
    flattened
    .select(
        "id",
        "plan",
        "val",
        F.percent_rank().over(w).alias("val_percent_rank"),
        F.cume_dist().over(w2).alias("val_cume_dist"),
    )
)

enriched = (
    metrics
    .select(
        "plan",
        "val_percent_rank",
        "val_cume_dist",
        F.stddev_samp("val").over(w_ordered).alias("plan_stddev_samp_val"),
        F.stddev_samp("val_percent_rank").over(w_ordered).alias("plan_stddev_samp_val_percent_rank"),
        F.count("val_percent_rank").over(w_ordered).alias("plan_count_val_percent_rank"),
        F.dense_rank().over(w4).alias("val_percent_rank_dense_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("plan").isin(["basic", "enterprise", "starter"]))
    .select(
        "*",
        F.round((F.col("val_cume_dist") / F.col("plan_count_val_percent_rank")), 2).alias("val_cume_dist_per_plan_count_val_percent_rank"),
    )
)

filtered = (
    cleaned
    .filter(F.col("val_percent_rank_dense_rank") == 5)
    .select(
        "*",
        F.round("plan_stddev_samp_val_percent_rank", 0).alias("plan_stddev_samp_val_percent_rank_rounded"),
    )
)

metrics_price_tiers = (
    filtered
    .select(
        "*",
        F.cume_dist().over(w_running).alias("val_percent_rank_cume_dist"),
        F.avg("val_percent_rank").over(w_ordered).alias("plan_avg_val_percent_rank"),
        F.sum("val_percent_rank_dense_rank").over(w_ordered).alias("plan_sum_val_percent_rank_dense_rank"),
        F.percent_rank().over(w6).alias("val_percent_rank_percent_rank"),
    )
)

report = (
    metrics_price_tiers
    .select(
        "val_percent_rank",
        "val_cume_dist",
        "plan_stddev_samp_val",
        "plan_stddev_samp_val_percent_rank",
        "plan_count_val_percent_rank",
        "plan_stddev_samp_val_percent_rank_rounded",
        "plan_avg_val_percent_rank",
        "val_percent_rank_percent_rank",
    )
)

result = (
    report
    .select(
        "val_cume_dist",
        "plan_stddev_samp_val_percent_rank_rounded",
        "val_percent_rank_percent_rank",
    )
    .orderBy(F.col("val_cume_dist").desc_nulls_last(), "val_percent_rank_percent_rank")
)
