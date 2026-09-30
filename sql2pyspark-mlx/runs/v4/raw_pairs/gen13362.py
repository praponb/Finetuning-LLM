from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("effective_from_day").desc_nulls_first())
w2 = Window.orderBy(F.col("tier_value").desc())
w_ordered = Window.orderBy(F.col("tier_value").asc_nulls_first())
w_recent = Window.orderBy(F.col("effective_from_day").desc())

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("tier_value"))
)

filtered = (
    exploded
    .filter(F.col("effective_from") >= "2023-06-01")
    .select("*", F.dayofmonth("effective_from").alias("effective_from_day"))
)

filtered_price_tiers = (
    filtered
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-02-26"))) & (F.col("effective_from_day") == 70))
    .withColumn("effective_from_day_tier", F.when(F.col("effective_from_day") >= 87, "High").when(F.col("effective_from_day") >= 31, "Medium").otherwise("Low"))
)

enriched = (
    filtered_price_tiers
    .select(
        "*",
        F.cume_dist().over(w).alias("effective_from_day_cume_dist"),
        F.rank().over(w2).alias("tier_value_rank"),
        F.rank().over(w_ordered).alias("tier_value_rank_2"),
        F.rank().over(w_recent).alias("effective_from_day_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "effective_from",
        "tier_value",
        "effective_from_day",
        "effective_from_day_tier",
        "tier_value_rank",
        "effective_from_day_rank",
    )
)
