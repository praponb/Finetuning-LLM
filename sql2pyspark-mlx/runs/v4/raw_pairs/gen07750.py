from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("order_date") >= "2025-04-01").alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_anti")
    .select(F.col("oi.id"), F.col("oi.order_id"), F.col("oi.unit_price"))
)
