from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(~((F.col("o.store_id") == 29) | F.col("o.amount").between(930, 1040)))
    .select(F.col("o.store_id"), F.col("oi.quantity"), F.col("oi.product_id"))
    .orderBy("store_id")
)
