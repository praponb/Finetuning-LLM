from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("tier_value")
w2 = Window.orderBy(F.col("adjusted_tier_value").desc_nulls_first())
w3 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w_grp = Window.orderBy("adjusted_tier_value_percent_rank")
w5 = Window.orderBy(F.col("adjusted_tier_value_per_tier_value_percent_rank").desc_nulls_last())
w_part = Window.orderBy(F.col("adjusted_tier_value_per_tier_value_percent_rank").desc())
w7 = Window.orderBy(F.col("adjusted_tier_value_per_tier_value_percent_rank").desc_nulls_first())

flattened = df_price_tiers.select("id", F.explode("discounts").alias("tier_value"))

cleaned = (
    flattened
    .filter(F.col("tier_value") < 41)
    .select("*", F.round((F.col("tier_value") * 1.1), 2).alias("adjusted_tier_value"))
)

enriched = (
    cleaned
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w))
    .withColumn("adjusted_tier_value_percent_rank", F.percent_rank().over(w2))
    .withColumn("tier_value_percent_rank_2", F.percent_rank().over(w3))
)

filtered = (
    enriched
    .filter(F.col("tier_value_percent_rank_2") > 0.74)
    .withColumn("tier_value_int", F.col("tier_value").cast("int"))
    .withColumn("adjusted_tier_value_per_tier_value_percent_rank", (F.col("adjusted_tier_value") / F.nullif("tier_value_percent_rank", F.lit(0))))
)

valid = (
    filtered
    .filter(F.col("adjusted_tier_value_percent_rank") > 0.56)
    .withColumn("adjusted_tier_value_minus_adjusted_tier_value_per_tier_value_percent_rank", (F.col("adjusted_tier_value") - F.col("adjusted_tier_value_per_tier_value_percent_rank")))
)

cleaned_price_tiers = (
    valid
    .filter(F.col("adjusted_tier_value").isNotNull())
    .select(
        "id",
        "tier_value",
        "tier_value_percent_rank",
        "adjusted_tier_value_percent_rank",
        "tier_value_percent_rank_2",
        "tier_value_int",
        "adjusted_tier_value_per_tier_value_percent_rank",
        "adjusted_tier_value_minus_adjusted_tier_value_per_tier_value_percent_rank",
        F.round("adjusted_tier_value_percent_rank", 0).alias("adjusted_tier_value_percent_rank_rounded"),
    )
)

enriched_price_tiers = (
    cleaned_price_tiers
    .select(
        "*",
        F.dense_rank().over(w_grp).alias("adjusted_tier_value_percent_rank_dense_rank"),
        F.rank().over(w5).alias("adjusted_tier_value_per_tier_value_percent_rank_rank"),
        F.dense_rank().over(w_part).alias("adjusted_tier_value_per_tier_value_percent_rank_dense_rank"),
        F.percent_rank().over(w7).alias("adjusted_tier_value_per_tier_value_percent_rank_percent_rank"),
    )
)

result = (
    enriched_price_tiers
    .select(
        "id",
        "tier_value",
        "tier_value_percent_rank",
        "adjusted_tier_value_percent_rank",
        "tier_value_percent_rank_2",
        "tier_value_int",
        "adjusted_tier_value_per_tier_value_percent_rank",
        "adjusted_tier_value_minus_adjusted_tier_value_per_tier_value_percent_rank",
        "adjusted_tier_value_percent_rank_rounded",
    )
)
