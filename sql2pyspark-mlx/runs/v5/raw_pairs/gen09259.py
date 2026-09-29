from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("carrier").isin(["Flash", "FedEx"]))
    .groupBy("order_id")
    .agg(
        F.max("cost").alias("max_cost"),
        F.max("delivered_date").alias("last_delivered_date"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.max_cost") > 56)
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("t.max_cost"),
        F.col("t.last_delivered_date"),
    )
)
