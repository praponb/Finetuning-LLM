from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.orderBy(F.col("tier_value_dense_rank").desc_nulls_first())
w3 = Window.orderBy(F.col("tier_value_percent_rank").desc())
w4 = Window.orderBy(F.col("tier_value_percent_rank_int").asc_nulls_last())
w5 = Window.orderBy("tier_value")
w6 = Window.orderBy(F.col("tier_value_percent_rank_cume_dist").asc_nulls_last())
w7 = Window.orderBy(F.col("tier_value_percent_rank_int_percent_rank").asc_nulls_first())
w8 = Window.orderBy(F.col("tier_value_percent_rank_cume_dist_percent_rank").desc_nulls_first())
w9 = Window.orderBy(F.col("tier_value_cume_dist").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_limits").alias("tier_value"))
)

metrics = lines.withColumn("tier_value_dense_rank", F.dense_rank().over(w))

enriched = (
    metrics
    .select(
        "*",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.rank().over(w2).alias("tier_value_dense_rank_rank"),
    )
)

filtered = (
    enriched
    .filter(F.col("effective_from") > F.lit("2024-02-26").cast("date"))
    .select(
        "id",
        "tier_value",
        "tier_value_percent_rank",
        F.col("tier_value_percent_rank").cast("int").alias("tier_value_percent_rank_int"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "tier_value",
        F.cume_dist().over(w3).alias("tier_value_percent_rank_cume_dist"),
        F.percent_rank().over(w4).alias("tier_value_percent_rank_int_percent_rank"),
        F.cume_dist().over(w5).alias("tier_value_cume_dist"),
    )
)

scored_price_tiers = (
    scored
    .withColumn("tier_value_percent_rank_cume_dist_percent_rank", F.percent_rank().over(w6))
    .withColumn("tier_value_percent_rank_int_percent_rank_percent_rank", F.percent_rank().over(w7))
)

scored_price_tiers_2 = (
    scored_price_tiers
    .filter(F.col("tier_value_percent_rank_int_percent_rank_percent_rank").between(0.23, 0.8))
    .select(
        "id",
        "tier_value",
        "tier_value_percent_rank_cume_dist",
        "tier_value_percent_rank_int_percent_rank",
        "tier_value_percent_rank_cume_dist_percent_rank",
        "tier_value_percent_rank_int_percent_rank_percent_rank",
        F.cume_dist().over(w8).alias("tier_value_percent_rank_cume_dist_percent_rank_cume_dist"),
        F.dense_rank().over(w9).alias("tier_value_cume_dist_dense_rank"),
    )
)

result = (
    scored_price_tiers_2
    .select(
        "id",
        "tier_value",
        "tier_value_percent_rank_int_percent_rank",
        "tier_value_percent_rank_int_percent_rank_percent_rank",
        "tier_value_percent_rank_cume_dist_percent_rank_cume_dist",
        "tier_value_cume_dist_dense_rank",
    )
    .orderBy(F.col("tier_value").desc_nulls_first())
)
