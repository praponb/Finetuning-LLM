from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_last())

exploded = df_price_tiers.select("id", F.explode("discounts").alias("tier_value"))

valid = (
    exploded
    .filter(F.col("tier_value") <= 63)
    .withColumn("tier_value_level", F.when(F.col("tier_value") >= 67, "High").when(F.col("tier_value") >= 19, "Medium").otherwise("Low"))
    .withColumn("tier_value_rounded", F.round("tier_value", 0))
)

remaining_keys = (
    valid
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.col("currency").isin(["USD", "THB", "EUR"]))
        .select("id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .filter(F.col("tier_value_rounded") <= 65)
    .select(
        "tier_value",
        "tier_value_rounded",
        F.rank().over(w).alias("tier_value_rank"),
        F.rank().over(w).alias("tier_value_rank_2"),
    )
)

valid_price_tiers = (
    with_windows
    .filter(F.col("tier_value_rounded").isNotNull())
    .withColumn("tier_value_rounded_rounded", F.round("tier_value_rounded", 2))
)

cleaned = (
    valid_price_tiers
    .filter(F.col("tier_value_rounded_rounded") < 42)
    .withColumn("tier_value_rank_plus_tier_value_rank_2", (F.col("tier_value_rank") + F.col("tier_value_rank_2")))
)

result = (
    cleaned
    .withColumn("tier_value_tier", F.when(F.col("tier_value") >= 88, "PREMIUM").otherwise("STANDARD"))
    .cube("tier_value_tier")
    .agg(
        F.max("tier_value_rounded").alias("max_tier_value_rounded"),
        F.percentile_approx("tier_value_rounded_rounded", [0.25, 0.5, 0.75]).alias("tier_value_rounded_rounded_quartiles"),
        F.grouping("tier_value_tier").alias("gid"),
    )
    .filter(F.col("max_tier_value_rounded") > 0)
)
