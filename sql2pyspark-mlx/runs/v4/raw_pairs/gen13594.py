from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_last())
w_part = Window.partitionBy("currency", "plan").orderBy(F.col("tier_value_cume_dist").desc())
w3 = Window.partitionBy("plan")
w_grp = Window.partitionBy("currency")
w5 = Window.partitionBy("currency").orderBy(F.col("tier_value_dense_rank").desc())
w_grp_2 = Window.partitionBy("plan").orderBy(F.col("tier_value_dense_rank").desc())
w7 = Window.partitionBy("plan", "currency")
w8 = Window.partitionBy("currency", "plan")
w_running = Window.orderBy(F.col("tier_value_dense_rank_share").desc())
w10 = Window.partitionBy("currency").orderBy(F.col("tier_value_dense_rank_dense_rank_share").asc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

scored = (
    flattened
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w))
)

metrics = (
    scored
    .select(
        "*",
        F.cume_dist().over(w_part).alias("tier_value_cume_dist_cume_dist"),
        F.round((F.col("tier_value_dense_rank") / F.sum("tier_value_dense_rank").over(w3)), 4).alias("tier_value_dense_rank_share"),
        F.stddev_samp("tier_value_dense_rank").over(w_grp).alias("currency_stddev_samp_tier_value_dense_rank"),
        F.percent_rank().over(w5).alias("tier_value_dense_rank_percent_rank"),
        F.dense_rank().over(w_grp_2).alias("tier_value_dense_rank_dense_rank"),
    )
)

metrics_price_tiers = (
    metrics
    .withColumn("currency_stddev_samp_tier_value_dense_rank_share", F.round((F.col("currency_stddev_samp_tier_value_dense_rank") / F.sum("currency_stddev_samp_tier_value_dense_rank").over(w7)), 4))
    .withColumn("currency_count_tier_value", F.count("tier_value").over(w_grp))
    .withColumn("tier_value_dense_rank_dense_rank_share", F.round((F.col("tier_value_dense_rank_dense_rank") / F.sum("tier_value_dense_rank_dense_rank").over(w8)), 4))
)

filtered = (
    metrics_price_tiers
    .filter(F.col("effective_from") >= F.lit("2024-02-17").cast("date"))
    .select(
        "*",
        F.greatest("tier_value_dense_rank_share", "tier_value_cume_dist_cume_dist").alias("greatest_tier_value_dense_rank_share_tier_value_cume_dist_cume_dist"),
        F.length("plan").alias("plan_length"),
    )
)

cleaned = (
    filtered
    .filter(F.col("effective_from") >= F.lit("2024-04-13").cast("date"))
    .withColumn("tier_value_dense_rank_percent_rank_int", F.col("tier_value_dense_rank_percent_rank").cast("int"))
    .withColumn("tier_value_cume_dist_cume_dist_tier", F.when(F.col("tier_value_cume_dist_cume_dist") >= 0.63, "High").when(F.col("tier_value_cume_dist_cume_dist") >= 0.39, "Medium").otherwise("Low"))
)

enriched = (
    cleaned
    .withColumn("tier_value_dense_rank_share_cume_dist", F.cume_dist().over(w_running))
    .withColumn("tier_value_dense_rank_dense_rank_share_cume_dist", F.cume_dist().over(w10))
)

result = (
    enriched
    .select(
        "id",
        "plan",
        "tier_value_cume_dist",
        "tier_value_cume_dist_cume_dist",
        "currency_stddev_samp_tier_value_dense_rank_share",
        "currency_count_tier_value",
        "tier_value_dense_rank_dense_rank_share",
        "greatest_tier_value_dense_rank_share_tier_value_cume_dist_cume_dist",
        "tier_value_dense_rank_percent_rank_int",
    )
    .orderBy(F.col("tier_value_cume_dist").desc())
)
