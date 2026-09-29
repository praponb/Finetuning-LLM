from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("t.carrier_values"),
        F.col("t.avg_cost"),
    )
    .orderBy(F.col("carrier_values").desc(), F.col("avg_cost").desc())
)
