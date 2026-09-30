from pyspark.sql import functions as F

cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

joined = (
    df_payments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select(
        "src.*",
        F.col("d.amount").alias("order_amount"),
        F.col("d.channel"),
        F.col("d.order_date"),
    )
)

result = (
    joined
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "order_amount",
        "channel",
        "order_date",
        F.col("max_shipment_cost"),
    )
    .orderBy(F.col("channel").desc(), "paid_at")
)
