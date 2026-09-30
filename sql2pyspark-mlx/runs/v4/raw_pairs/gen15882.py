from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "left")
    .groupBy(F.col("orders.customer_id"), F.col("orders.channel"))
    .agg(F.max(F.col("order_items.quantity")).alias("max_quantity"))
    .orderBy("customer_id")
)
