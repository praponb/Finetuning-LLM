from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("tier_value"))
)

enriched = (
    exploded
    .filter(F.col("plan") == "enterprise")
    .select("*", F.cume_dist().over(w).alias("tier_value_cume_dist"))
)

cleaned = (
    enriched
    .filter(F.col("effective_from") > F.lit("2024-03-17").cast("date"))
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value",
        "tier_value_cume_dist",
        F.date_sub("effective_from", 65).alias("effective_from_minus_65d"),
    )
)

base = (
    cleaned
    .filter(F.col("effective_from") < "2025-09-01")
    .withColumn("effective_from_weekofyear", F.weekofyear("effective_from"))
)

result = (
    base
    .select(
        "id",
        "plan",
        "effective_from",
        "effective_from_minus_65d",
        "effective_from_weekofyear",
    )
    .orderBy(F.col("id").asc_nulls_first())
)
