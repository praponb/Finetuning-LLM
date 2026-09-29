from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("element"),
    )
)

result = (
    exploded
    .select("effective_from", "currency", "element")
    .orderBy(F.col("effective_from").desc_nulls_first())
)
