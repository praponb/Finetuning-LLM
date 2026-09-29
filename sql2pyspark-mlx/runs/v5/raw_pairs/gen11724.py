from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    exploded
    .select("id", "effective_from")
    .orderBy(F.col("id").desc_nulls_last())
)
