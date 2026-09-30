from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.count("cost").alias("cost_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "left")
    .select(
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("sbo.cost_count"),
        F.col("sbo.status_list"),
    )
)
