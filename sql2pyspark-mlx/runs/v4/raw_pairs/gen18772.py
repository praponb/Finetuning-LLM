from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .select(
        F.col("order_items.id"),
        F.col("order_items.order_id"),
        F.col("order_items.unit_price"),
        F.col("products.launch_date"),
    )
)
