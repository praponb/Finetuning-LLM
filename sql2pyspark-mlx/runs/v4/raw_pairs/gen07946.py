from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.filter(F.col("unit_price").between(270, 370)).alias("order_items"), F.col("products.id") == F.col("order_items.product_id"), "left_anti")
    .select(F.col("products.id"), F.col("products.stock_qty"))
)
