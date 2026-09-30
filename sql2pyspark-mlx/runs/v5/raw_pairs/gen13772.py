from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(160, 200))
    .select(
        "id",
        "order_id",
        "unit_price",
        "product_id",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)
