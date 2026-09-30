from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

result = (
    flattened
    .select("plan", "effective_from", "currency", "val")
    .orderBy(F.col("effective_from").desc(), F.col("val").desc())
)
