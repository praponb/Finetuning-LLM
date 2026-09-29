from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .groupBy(F.col("products.category"))
    .agg(F.avg(F.col("order_items.quantity")).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 7.1)
)
