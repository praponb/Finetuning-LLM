from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select(
        "id",
        "quantity",
        "order_id",
        "unit_price",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)
