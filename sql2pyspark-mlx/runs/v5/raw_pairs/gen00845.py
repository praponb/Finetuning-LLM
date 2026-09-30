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
    .select("plan", "effective_from", "currency")
    .orderBy(F.col("effective_from").asc_nulls_first())
)
