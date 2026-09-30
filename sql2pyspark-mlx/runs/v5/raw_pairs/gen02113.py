from pyspark.sql import functions as F

shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

kept_keys = (
    df_subscriptions
    .select("customer_id", "id")
    .intersect(
        df_subscriptions
        .filter(F.col("start_date") <= "2024-12-01")
        .select("customer_id", "id")
    )
)

verified = (
    df_subscriptions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "plan",
        "end_date",
        "status",
        F.col("min_order_shipping_fee"),
    )
)
