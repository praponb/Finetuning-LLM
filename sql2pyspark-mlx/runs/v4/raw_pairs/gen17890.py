from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    exploded
    .select("id", "plan", "currency", "element")
    .orderBy(F.col("currency").desc())
)
