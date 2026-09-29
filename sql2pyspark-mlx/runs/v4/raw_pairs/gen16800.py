from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "web", "store"]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.round("shipping_fee", 1).alias("shipping_fee_rounded"),
        F.date_format("order_date", "yyyy-MM").alias("order_date_label"),
    )
)

result = (
    base
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.order_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)
