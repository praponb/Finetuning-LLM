from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("unit_price") >= 260)
    .groupBy("order_id")
    .agg(F.countDistinct("quantity").alias("unique_orders"))
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "left")
    .filter(F.col("order_stats.unique_orders") > 5)
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("order_stats.unique_orders"),
    )
    .orderBy(F.col("order_date").desc())
)
