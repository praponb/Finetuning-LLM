from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.alias("order_items"), F.col("products.id") == F.col("order_items.product_id"), "left")
    .select(
        F.col("products.product_name"),
        F.col("products.stock_qty"),
        F.col("order_items.order_id"),
        F.col("order_items.unit_price"),
        F.col("order_items.quantity"),
        F.lower(F.col("products.category")).alias("category_lower"),
    )
)
