from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.orderBy(F.col("tier_value").asc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

enriched = (
    flattened
    .select(
        "id",
        "currency",
        "tier_value",
        F.max("tier_value").over(w).alias("currency_max_tier_value"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
    )
)

scored = enriched.select("id", "currency", F.rank().over(w3).alias("tier_value_rank"))

cleaned = (
    scored
    .filter(F.col("tier_value_rank") <= 3)
    .select(
        "*",
        F.when(F.col("tier_value_rank") >= 7, "High").when(F.col("tier_value_rank") >= 2, "Medium").otherwise("Low").alias("tier_value_rank_band"),
    )
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("tier_value_rank") >= 5)
    .select("*", F.ceil("tier_value_rank").alias("tier_value_rank_whole"))
)

remaining_keys = (
    cleaned_price_tiers
    .select("currency", "id")
    .intersect(
        df_price_tiers
        .filter(F.year("effective_from") >= 2025)
        .select("currency", "id")
    )
)

verified = (
    cleaned_price_tiers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("tier_value_rank_whole") >= 49)
    .select(
        "id",
        "currency",
        "tier_value_rank",
        "tier_value_rank_band",
        "tier_value_rank_whole",
        (F.col("tier_value_rank") / F.nullif("tier_value_rank_whole", F.lit(0))).alias("tier_value_rank_per_tier_value_rank_whole"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("tier_value_rank") >= 7, "LARGE").otherwise("SMALL").alias("tier_value_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.min("tier_value_rank_per_tier_value_rank_whole").alias("min_tier_value_rank_per_tier_value_rank_whole"),
    )
    .orderBy(F.col("tier_value_rank_tier").desc_nulls_last())
)
