from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("element"))
)

result = (
    flattened
    .select("id", "currency", "element")
    .orderBy(F.col("currency").asc_nulls_last())
)
