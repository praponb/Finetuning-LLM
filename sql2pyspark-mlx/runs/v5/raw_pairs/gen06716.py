from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_first())
w2 = Window.orderBy(F.col("val").desc_nulls_first())
w3 = Window.orderBy("val")
w4 = Window.partitionBy("plan")
w5 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())
w6 = Window.partitionBy("plan").orderBy(F.col("val_cume_dist").asc_nulls_last())
w_part = Window.orderBy(F.col("val_rank").asc_nulls_first())
w8 = Window.partitionBy("plan").orderBy(F.col("plan_count_val").desc_nulls_first())
w9 = Window.partitionBy("plan").orderBy("val_rank")
w10 = Window.partitionBy("plan").orderBy(F.col("val_rank").desc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("val"))
)

enriched = (
    exploded
    .select(
        "id",
        "plan",
        "effective_from",
        "val",
        F.cume_dist().over(w).alias("val_cume_dist"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "plan",
        "effective_from",
        "val",
        "val_cume_dist",
        F.rank().over(w2).alias("val_rank"),
        F.cume_dist().over(w3).alias("val_cume_dist_2"),
        F.count("val").over(w4).alias("plan_count_val"),
        F.percent_rank().over(w5).alias("val_percent_rank"),
        F.rank().over(w6).alias("val_cume_dist_rank"),
    )
)

metrics = (
    with_windows
    .filter(F.col("effective_from") > "2023-07-01")
    .withColumn("val_rank_dense_rank", F.dense_rank().over(w_part))
    .withColumn("plan_min_val_cume_dist", F.min("val_cume_dist").over(w4))
    .withColumn("plan_count_val_cume_dist", F.cume_dist().over(w8))
    .withColumn("val_rank_rank", F.rank().over(w9))
    .withColumn("val_dense_rank", F.dense_rank().over(w2))
)

enriched_price_tiers = (
    metrics
    .select(
        "*",
        F.stddev_samp("val_rank").over(w4).alias("plan_stddev_samp_val_rank"),
        F.percent_rank().over(w10).alias("val_rank_percent_rank"),
    )
)

summary = (
    enriched_price_tiers
    .withColumn("week_start", F.date_trunc("week", "effective_from"))
    .withColumn("plan_count_val_tier", F.when(F.col("plan_count_val") >= 17, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["week_start", "plan_count_val_tier"], ["week_start"], []],
        "week_start",
        "plan_count_val_tier",
    )
    .agg(
        F.percentile_approx("val_cume_dist", [0.25, 0.5, 0.75]).alias("val_cume_dist_quartiles"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.round(F.avg("val_cume_dist_2"), 2).alias("avg_val_cume_dist_2"),
        F.percentile_approx("plan_min_val_cume_dist", 0.9).alias("plan_min_val_cume_dist_p90"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.grouping("week_start").alias("gid"),
    )
    .filter(F.col("plan_min_val_cume_dist_p90") >= 0)
)

result = (
    summary
    .select(
        "week_start",
        "plan_count_val_tier",
        "val_cume_dist_quartiles",
        "avg_val_cume_dist_2",
        "plan_min_val_cume_dist_p90",
        "distinct_plans",
        "gid",
    )
    .orderBy(F.col("plan_count_val_tier").desc_nulls_last())
)
