from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.size(F.col("p.tags")) > 1)
    .groupBy(F.month(F.col("p.launch_date")).alias("launch_month"))
    .agg(F.count("*").alias("order_item_count"))
    .orderBy("order_item_count")
)
