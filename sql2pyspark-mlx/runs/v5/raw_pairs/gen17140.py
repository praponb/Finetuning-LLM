from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("val"),
    )
)

result = (
    exploded
    .select("id", "plan", "effective_from", "currency", "val")
    .orderBy(F.col("id").desc())
)
