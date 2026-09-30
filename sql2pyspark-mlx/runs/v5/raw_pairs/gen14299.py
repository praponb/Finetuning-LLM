from pyspark.sql import functions as F

cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

cleaned = (
    df_payments
    .filter(F.col("paid_at") >= "2023-03-01")
    .withColumn("amount_int", F.col("amount").cast("int"))
    .withColumn("method_filled", F.coalesce("method", F.lit("N/A")))
)

kept_keys = (
    cleaned
    .select("status")
    .exceptAll(
        df_payments
        .filter(F.col("amount") >= 460)
        .select("status")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.discount") >= 0.37)
    .select("e.*", F.col("d.status").alias("order_status"))
)

joined = (
    enriched
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "src.order_id",
        "src.method",
        "src.order_status",
        F.col("b.channel"),
        F.col("b.shipping_fee"),
    )
)

result = (
    joined
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "method",
        "order_status",
        "channel",
        "shipping_fee",
        F.when(F.col("channel").rlike("^(mobile|partner)$"), "GROUP_B").when(F.col("channel").rlike("^(web|store)$"), "GROUP_A").otherwise("PRIORITY").alias("channel_zone"),
        F.col("max_shipment_cost"),
    )
)
