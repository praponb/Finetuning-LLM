from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.year("delivered_date") == 2025)
    .groupBy("order_id")
    .agg(
        F.max("delivered_date").alias("last_delivered_date"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("os.last_delivered_date"),
        F.col("os.status_list"),
    )
    .orderBy("last_delivered_date")
)
