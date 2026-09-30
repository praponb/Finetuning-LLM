from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.alias("order_items"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .select(
        F.col("products.price"),
        F.col("products.category"),
        F.col("order_items.order_id"),
        F.col("order_items.quantity"),
    )
)
