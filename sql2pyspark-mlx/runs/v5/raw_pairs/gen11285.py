from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(F.countDistinct(F.col("oi.quantity")).alias("unique_products"))
    .orderBy(F.col("unique_products").desc(), "status")
    .limit(3)
)
