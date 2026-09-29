from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.max("delivered_date").alias("last_delivered_date"),
        F.sum(F.when(F.col("carrier") == "UPS", F.col("cost")).otherwise(0)).alias("ups_cost"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "left")
    .filter(F.col("order_stats.ups_cost") >= 40)
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("orders.status"),
        F.col("order_stats.last_delivered_date"),
        F.col("order_stats.ups_cost"),
    )
    .orderBy("status")
)
