from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("val"),
    )
)

result = (
    exploded
    .select("id", "plan", "currency")
    .orderBy(F.col("id").desc())
)
