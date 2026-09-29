from pyspark.sql import functions as F

shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("shipping_fee").alias("avg_order_shipping_fee"))

kept_keys = (
    df_orders
    .select("channel", "id")
    .intersectAll(
        df_orders
        .filter(F.col("store_id") == 38)
        .select("channel", "id")
    )
)

reconciled = (
    df_orders
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.col("avg_order_shipping_fee"),
    )
)
