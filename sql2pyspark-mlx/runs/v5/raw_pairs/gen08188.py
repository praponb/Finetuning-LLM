from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())
w_grp = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value_percent_rank_per_tier_value").desc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("tier_value"))
)

scored = (
    exploded
    .filter(F.col("currency").isin(["EUR", "THB"]))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w_grp))
)

filtered = (
    scored
    .filter(F.col("tier_value_percent_rank") < 0.66)
    .select(
        "id",
        "plan",
        "currency",
        "tier_value",
        "tier_value_cume_dist",
        "tier_value_percent_rank",
        (F.col("tier_value_percent_rank") / F.nullif("tier_value", F.lit(0))).alias("tier_value_percent_rank_per_tier_value"),
    )
)

enriched = (
    filtered
    .select(
        "plan",
        "currency",
        "tier_value_cume_dist",
        F.cume_dist().over(w3).alias("tier_value_percent_rank_per_tier_value_cume_dist"),
    )
)

result = (
    enriched
    .select(
        "plan",
        "tier_value_cume_dist",
        "tier_value_percent_rank_per_tier_value_cume_dist",
    )
    .orderBy(F.col("tier_value_cume_dist").desc())
)
