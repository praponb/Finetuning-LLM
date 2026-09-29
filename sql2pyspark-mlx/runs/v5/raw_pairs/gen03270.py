from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .groupBy("order_id")
    .agg(
        F.count("cost").alias("cost_count"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("order_summary"), F.col("orders.id") == F.col("order_summary.order_id"), "inner")
    .filter(F.col("order_summary.cost_count") > 8)
    .select(
        F.col("orders.id"),
        F.col("orders.status"),
        F.col("order_summary.cost_count"),
        F.col("order_summary.unique_carriers"),
    )
    .orderBy("id")
    .limit(5)
)
