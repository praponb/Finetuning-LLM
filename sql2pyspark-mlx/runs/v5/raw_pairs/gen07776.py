from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.alias("order_items"), F.col("products.id") == F.col("order_items.product_id"), "left_anti")
    .filter(F.col("products.product_name").isNotNull())
    .select(F.col("products.id"), F.col("products.stock_qty"), F.col("products.brand"))
    .orderBy("stock_qty")
)
