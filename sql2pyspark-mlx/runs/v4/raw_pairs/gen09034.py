from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_limits").alias("tier_value"))
)

kept_keys = (
    lines
    .select("currency", "id")
    .subtract(
        df_price_tiers
        .filter(F.col("plan") == "basic")
        .select("currency", "id")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "*",
        F.percent_rank().over(w_part).alias("tier_value_percent_rank"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
    )
)

filtered = (
    scored
    .filter(F.col("currency").isin(["THB", "USD"]))
    .select(
        "id",
        "currency",
        "tier_value",
        "tier_value_percent_rank",
        F.when(F.col("tier_value") >= 79, "High").when(F.col("tier_value") >= 11, "Medium").otherwise("Low").alias("tier_value_tier"),
    )
)

result = (
    filtered
    .withColumn("tier_value_percent_rank_tier", F.when(F.col("tier_value_percent_rank") >= 0.64, "LARGE").otherwise("SMALL"))
    .withColumn("tier_value_tier", F.when(F.col("tier_value") >= 60, "PREMIUM").otherwise("STANDARD"))
    .cube("tier_value_percent_rank_tier", "tier_value_tier")
    .agg(
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.sum(F.when(F.col("currency") == "THB", F.col("tier_value")).otherwise(0)).alias("thb_tier_value"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("tier_value_tier").asc_nulls_first())
)
