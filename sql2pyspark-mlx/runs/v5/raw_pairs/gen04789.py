from pyspark.sql import functions as F

flattened = (
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
    flattened
    .select("id", "effective_from")
    .orderBy(F.col("id").asc_nulls_last())
)
