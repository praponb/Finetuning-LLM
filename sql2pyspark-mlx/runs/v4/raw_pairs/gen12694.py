from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2024-02-22").cast("date")) & F.col("shipping_fee").isNotNull())
    .select(
        "id",
        "customer_id",
        "status",
        "channel",
        "amount",
        "discount",
        F.concat_ws(",", "channel", "status").alias("channel_status_key"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") < "2025-05-01"), "left_anti")
)
