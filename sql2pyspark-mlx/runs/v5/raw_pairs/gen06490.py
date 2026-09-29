from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("discounts").alias("tier_value"))
)

result = (
    lines
    .select("id", "tier_value")
    .orderBy(F.col("id").asc_nulls_first())
)
