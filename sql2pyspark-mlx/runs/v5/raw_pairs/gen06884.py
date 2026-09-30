from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("currency")
w3 = Window.orderBy("currency_max_tier_value")
w4 = Window.partitionBy("currency").orderBy(F.col("tier_value_dense_rank_share_2").desc_nulls_first())
w5 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("tier_value") < 28)
    .select("*", F.date_trunc("month", "effective_from").alias("effective_from_period"))
)

scored = cleaned.withColumn("tier_value_dense_rank", F.dense_rank().over(w))

enriched = (
    scored
    .filter(F.col("tier_value").between(26, 66))
    .select(
        "currency",
        "tier_value",
        "effective_from_period",
        "tier_value_dense_rank",
        F.round((F.col("tier_value_dense_rank") / F.sum("tier_value_dense_rank").over(w2)), 4).alias("tier_value_dense_rank_share"),
        F.round((F.col("tier_value_dense_rank") / F.sum("tier_value_dense_rank").over(w2)), 4).alias("tier_value_dense_rank_share_2"),
        F.max("tier_value").over(w2).alias("currency_max_tier_value"),
    )
)

scored_price_tiers = (
    enriched
    .withColumn("currency_max_tier_value_dense_rank", F.dense_rank().over(w3))
    .withColumn("tier_value_dense_rank_share_2_dense_rank", F.dense_rank().over(w4))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w5))
)

daily = (
    scored_price_tiers
    .withColumn("week_start", F.date_trunc("WEEK", "effective_from_period"))
    .withColumn("tier_value_dense_rank_tier", F.when(F.col("tier_value_dense_rank") >= 8, "PREMIUM").otherwise("STANDARD"))
    .cube("week_start", "tier_value_dense_rank_tier")
    .agg(
        F.percentile_approx("currency_max_tier_value_dense_rank", [0.25, 0.5, 0.75]).alias("currency_max_tier_value_dense_rank_quartiles"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("distinct_currencies") >= 1)
)

result = (
    daily
    .select("week_start", "num_currencies", "distinct_currencies", "grouping_level")
    .orderBy(F.col("grouping_level").desc(), "week_start")
)
