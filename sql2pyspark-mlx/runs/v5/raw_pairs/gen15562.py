from pyspark.sql import functions as F

shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("shipping_fee").alias("sum_order_shipping_fee"))

kept_keys = (
    df_subscriptions
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date") < "2024-10-01")
        .select("status")
    )
)

reconciled = (
    df_subscriptions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.col("sum_order_shipping_fee"),
    )
    .orderBy(F.col("sum_order_shipping_fee").asc_nulls_last(), F.col("end_date").desc())
)
