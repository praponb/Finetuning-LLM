from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

result = (
    lines
    .select("id", "effective_from", "currency")
    .orderBy(F.col("id").desc())
)
