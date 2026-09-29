from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([5, 24]))
    .select(
        "order_id",
        "product_id",
        F.round("unit_price", 2).alias("unit_price_rounded"),
    )
    .orderBy(F.col("unit_price_rounded").desc())
)
