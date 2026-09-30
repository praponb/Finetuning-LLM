from pyspark.sql import functions as F

shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("shipping_fee").alias("max_order_shipping_fee"))

matched_keys = (
    df_orders
    .select("status")
    .intersectAll(
        df_orders
        .filter(F.col("customer_id").isNotNull())
        .select("status")
    )
)

kept_rows = (
    df_orders
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.col("max_order_shipping_fee"),
    )
    .orderBy(F.col("shipping_fee").desc(), "status")
)
