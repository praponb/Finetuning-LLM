from pyspark.sql import functions as F

cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

with_dims = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isin([16, 34]))
    .select("a.*", F.col("o.order_date"), F.col("o.channel"), F.col("o.shipping_fee"))
)

result = (
    with_dims
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        F.col("sum_shipment_cost"),
    )
    .orderBy(F.col("order_id").desc())
)
