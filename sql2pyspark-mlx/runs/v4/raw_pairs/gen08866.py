from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter((F.col("o.store_id") == 32) & F.col("oi.product_id").isin([4, 15, 21]))
    .select(F.col("o.store_id"), F.col("oi.product_id"), F.col("oi.order_id"))
    .orderBy(F.col("store_id").desc())
)
