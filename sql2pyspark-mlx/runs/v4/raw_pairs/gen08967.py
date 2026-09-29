from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("discounts").alias("tier_value"))
)

result = (
    lines
    .select("id", "plan", "tier_value")
    .orderBy(F.col("plan").desc_nulls_last())
)
