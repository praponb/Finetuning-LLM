from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "IN_TRANSIT", "DELIVERED"]))
    .groupBy("order_id")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max("shipped_date").alias("latest_shipped_date"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("order_summary"), F.col("orders.id") == F.col("order_summary.order_id"), "inner")
    .select(
        F.col("orders.store_id"),
        F.col("order_summary.status_list"),
        F.col("order_summary.latest_shipped_date"),
    )
    .orderBy("latest_shipped_date")
)
