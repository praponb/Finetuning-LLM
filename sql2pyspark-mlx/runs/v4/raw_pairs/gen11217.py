from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("element"),
    )
)

result = (
    flattened
    .select("id", "effective_from", "currency", "element")
    .orderBy(F.col("currency").desc_nulls_first(), F.col("effective_from").desc())
)
