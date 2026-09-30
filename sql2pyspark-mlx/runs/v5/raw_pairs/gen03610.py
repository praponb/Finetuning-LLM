from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("element")
w2 = Window.orderBy(F.col("element").desc_nulls_last())
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("plan", "currency").orderBy(F.col("element").desc_nulls_last())
w5 = Window.partitionBy("plan", "currency").orderBy(F.col("element").desc_nulls_first())
w_recent = Window.partitionBy("plan").orderBy(F.col("element_rank").desc_nulls_first())
w7 = Window.partitionBy("plan").orderBy(F.col("element_rank").desc())
w8 = Window.orderBy(F.col("plan_max_element").asc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("element"))
)

enriched = (
    exploded
    .select(
        "id",
        "plan",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
        F.max("element").over(w3).alias("plan_max_element"),
        F.percent_rank().over(w4).alias("element_percent_rank_2"),
        F.rank().over(w5).alias("element_rank"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "element_percent_rank_2",
        F.round((F.col("element_dense_rank") / F.sum("element_dense_rank").over(w3)), 4).alias("element_dense_rank_share"),
        F.dense_rank().over(w_recent).alias("element_rank_dense_rank"),
        F.cume_dist().over(w7).alias("element_rank_cume_dist"),
        F.rank().over(w8).alias("plan_max_element_rank"),
        F.dense_rank().over(w_recent).alias("element_rank_dense_rank_2"),
    )
)

base = (
    with_windows
    .filter(F.col("element_percent_rank_2") > 0.61)
    .select(
        "id",
        "element_dense_rank_share",
        "element_rank_dense_rank",
        "element_rank_cume_dist",
        "plan_max_element_rank",
        "element_rank_dense_rank_2",
        F.ceil("element_rank_dense_rank").alias("element_rank_dense_rank_whole"),
    )
)

base_price_tiers = (
    base
    .filter(F.col("plan_max_element_rank") == 5)
    .withColumn("element_dense_rank_share_rounded", F.round("element_dense_rank_share", 2))
)

valid = (
    base_price_tiers
    .filter(F.col("element_rank_dense_rank_2") > 3)
    .select(
        "*",
        (F.col("element_dense_rank_share") / F.nullif("element_dense_rank_share_rounded", F.lit(0))).alias("element_dense_rank_share_per_element_dense_rank_share_rounded"),
    )
)

result = (
    valid
    .withColumn("element_rank_dense_rank_whole_tier", F.when(F.col("element_rank_dense_rank_whole") >= 70, "HIGH").otherwise("LOW"))
    .withColumn("element_dense_rank_share_per_element_dense_rank_share_rounded_tier", F.when(F.col("element_dense_rank_share_per_element_dense_rank_share_rounded") >= 80, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["element_rank_dense_rank_whole_tier", "element_dense_rank_share_per_element_dense_rank_share_rounded_tier"], ["element_rank_dense_rank_whole_tier"], []],
        "element_rank_dense_rank_whole_tier",
        "element_dense_rank_share_per_element_dense_rank_share_rounded_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("element_rank_dense_rank", 0.9).alias("element_rank_dense_rank_p90"),
        F.count("*").alias("row_count"),
        F.grouping("element_rank_dense_rank_whole_tier").alias("grouping_level"),
    )
    .orderBy(F.col("row_count").desc_nulls_first())
)
