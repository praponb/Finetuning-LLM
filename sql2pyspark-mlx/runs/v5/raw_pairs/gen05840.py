from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "quantity",
        "product_id",
        "order_id",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)
