from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("OrderStats"), F.col("orders.id") == F.col("OrderStats.order_id"), "inner")
    .filter(F.col("OrderStats.avg_cost") > 50)
    .select(
        F.col("orders.id"),
        F.col("orders.status"),
        F.col("orders.order_date"),
        F.col("OrderStats.avg_cost"),
        F.col("OrderStats.unique_carriers"),
    )
)
