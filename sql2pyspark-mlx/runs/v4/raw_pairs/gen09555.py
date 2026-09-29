from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("unit_price") < 170).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_anti")
    .filter(F.col("o.customer_id").isNotNull())
    .select(F.col("o.id"), F.col("o.customer_id"), F.col("o.order_date"))
)
