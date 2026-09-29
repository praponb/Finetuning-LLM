from pyspark.sql import functions as F

joined = (
    df_payments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select(
        "e.*",
        F.col("o.channel"),
        F.col("o.amount").alias("order_amount"),
        F.col("o.shipping_fee"),
    )
)

result = (
    joined
    .select(
        "id",
        "paid_at",
        "status",
        "channel",
        "order_amount",
        "shipping_fee",
        F.when(F.col("channel").rlike("^(partner|store)$"), "PRIORITY").otherwise("CORE").alias("channel_zone"),
    )
)
