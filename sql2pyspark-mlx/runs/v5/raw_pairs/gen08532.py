from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.store_id"))
    .agg(F.countDistinct(F.col("oi.quantity")).alias("unique_orders"))
    .orderBy("unique_orders", "store_id")
    .limit(10)
)
