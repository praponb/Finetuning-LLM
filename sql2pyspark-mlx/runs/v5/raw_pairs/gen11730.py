from pyspark.sql import functions as F

shipping_fee_by_store_id = df_orders.groupBy("store_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

kept = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") < F.col("s.order_date")), "left_semi")
)

result = (
    kept
    .join(shipping_fee_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.when(F.col("channel").rlike("^(mobile|web)$"), "EXTENDED").when(F.col("channel").rlike("^(store|partner)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("channel_zone"),
        F.col("min_order_shipping_fee"),
    )
    .orderBy(F.col("channel").desc())
)
