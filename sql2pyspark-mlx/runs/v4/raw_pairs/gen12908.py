from pyspark.sql import functions as F

cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

enriched = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.*", F.col("o.discount"), F.col("o.channel"))
)

result = (
    enriched
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "discount",
        "channel",
        F.when(F.col("channel").rlike("^(mobile|store)$"), "CORE").otherwise("GROUP_B").alias("channel_zone"),
        F.col("min_shipment_cost"),
    )
)
