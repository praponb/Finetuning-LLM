from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency").orderBy(F.col("val").asc_nulls_last())
w2 = Window.orderBy(F.col("val").desc_nulls_first())
w_hist = Window.partitionBy("plan")
w_grp = Window.orderBy("val_cume_dist")
w_part = Window.partitionBy("currency", "plan")
w6 = Window.partitionBy("currency").orderBy(F.col("val_dense_rank").desc())
w7 = Window.orderBy(F.col("val_dense_rank").desc_nulls_first())
w8 = Window.partitionBy("currency")
w9 = Window.partitionBy("plan", "currency").orderBy(F.col("val_dense_rank_dense_rank").desc_nulls_first())
w10 = Window.partitionBy("currency", "plan").orderBy(F.col("val_dense_rank_cume_dist").desc_nulls_first())
w11 = Window.partitionBy("currency").orderBy("val_dense_rank_cume_dist_cume_dist")
w12 = Window.orderBy(F.col("plan_count_val_dense_rank_cume_dist_cume_dist").desc())
w13 = Window.partitionBy("plan").orderBy(F.col("val_dense_rank_dense_rank_share").desc_nulls_last())
w14 = Window.partitionBy("currency").orderBy(F.col("val_dense_rank_dense_rank").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("val"),
    )
)

scored = (
    lines
    .withColumn("val_cume_dist", F.cume_dist().over(w))
    .withColumn("val_dense_rank", F.dense_rank().over(w2))
)

with_windows = (
    scored
    .withColumn("plan_count_val_cume_dist", F.count("val_cume_dist").over(w_hist))
    .withColumn("plan_max_val_cume_dist", F.max("val_cume_dist").over(w_hist))
    .withColumn("val_cume_dist_rank", F.rank().over(w_grp))
    .withColumn("currency_avg_val_dense_rank", F.avg("val_dense_rank").over(w_part))
    .withColumn("val_dense_rank_cume_dist", F.cume_dist().over(w6))
)

metrics = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w7).alias("val_dense_rank_dense_rank"),
        F.sum("plan_count_val_cume_dist").over(w8).alias("currency_sum_plan_count_val_cume_dist"),
    )
)

with_windows_price_tiers = (
    metrics
    .withColumn("val_dense_rank_dense_rank_rank", F.rank().over(w9))
    .withColumn("plan_count_val", F.count("val").over(w_hist))
    .withColumn("val_dense_rank_cume_dist_cume_dist", F.cume_dist().over(w10))
)

enriched = (
    with_windows_price_tiers
    .select(
        "*",
        F.min("val_dense_rank_dense_rank_rank").over(w_hist).alias("plan_min_val_dense_rank_dense_rank_rank"),
        F.round((F.col("val_dense_rank_dense_rank") / F.sum("val_dense_rank_dense_rank").over(w8)), 4).alias("val_dense_rank_dense_rank_share"),
        F.count("val_dense_rank_cume_dist_cume_dist").over(w_hist).alias("plan_count_val_dense_rank_cume_dist_cume_dist"),
        F.percent_rank().over(w11).alias("val_dense_rank_cume_dist_cume_dist_percent_rank"),
    )
)

scored_price_tiers = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w12).alias("plan_count_val_dense_rank_cume_dist_cume_dist_percent_rank"),
        F.dense_rank().over(w13).alias("val_dense_rank_dense_rank_share_dense_rank"),
        F.dense_rank().over(w14).alias("val_dense_rank_dense_rank_dense_rank"),
    )
)

result = (
    scored_price_tiers
    .groupBy(
        F.when(F.col("currency_sum_plan_count_val_cume_dist") >= 84, "LARGE").otherwise("SMALL").alias("currency_sum_plan_count_val_cume_dist_tier"),
        F.when(F.col("val_dense_rank_dense_rank_share_dense_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("val_dense_rank_dense_rank_share_dense_rank_tier"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("plan_count_val_dense_rank_cume_dist_cume_dist_percent_rank")).otherwise(0)).alias("enterprise_plan_count_val_dense_rank_cume_dist_cume_dist_percent_rank"),
        F.percentile_approx("plan_min_val_dense_rank_dense_rank_rank", [0.25, 0.5, 0.75]).alias("plan_min_val_dense_rank_dense_rank_rank_quartiles"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.count("*").alias("event_count"),
    )
    .orderBy("event_count")
)
