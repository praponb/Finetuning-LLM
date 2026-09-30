from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 38)
    .select(
        "id",
        "order_id",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)
