from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("oi.product_id").isin([10, 16, 26, 37]))
    .groupBy(F.col("c.is_active"))
    .agg(F.min(F.col("oi.quantity")).alias("min_quantity"))
    .orderBy("min_quantity", "is_active")
    .limit(20)
)
