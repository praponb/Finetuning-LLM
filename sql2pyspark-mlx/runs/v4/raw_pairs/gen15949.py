from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_limits").alias("val"))
)

result = (
    flattened
    .select("id", "effective_from", "currency")
    .orderBy(F.col("id").desc_nulls_first(), F.col("currency").desc())
)
