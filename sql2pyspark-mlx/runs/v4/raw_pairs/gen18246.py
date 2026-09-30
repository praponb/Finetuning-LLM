from pyspark.sql import functions as F

lines = (
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
    lines
    .select("id", "plan", "effective_from", "currency", "val")
    .orderBy(F.col("plan").asc_nulls_first())
)
