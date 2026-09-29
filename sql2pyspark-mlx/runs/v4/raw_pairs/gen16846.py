from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") < 4) & (F.col("order_id") == 9) & (F.col("quantity") >= 6))
    .select(
        "id",
        "unit_price",
        "product_id",
        F.round("quantity", 0).alias("quantity_rounded"),
    )
    .orderBy(F.col("unit_price").desc(), "id")
    .limit(10)
)
