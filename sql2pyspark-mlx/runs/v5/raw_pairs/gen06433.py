from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.max("cost").alias("max_cost"),
    )
)

result = (
    df_orders.alias("orders")
    .join(shipments_by_order.alias("shipments_by_order"), F.col("orders.id") == F.col("shipments_by_order.order_id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.status"),
        F.col("orders.customer_id"),
        F.col("shipments_by_order.status_values"),
        F.col("shipments_by_order.max_cost"),
    )
    .orderBy(F.col("status").desc())
)
