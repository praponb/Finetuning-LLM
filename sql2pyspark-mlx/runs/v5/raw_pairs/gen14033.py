from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter(F.col("method").isNotNull() & (F.col("status") != "SUCCESS"))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.col("paid_at").cast("date").alias("paid_at_day"),
        F.concat_ws("-", "method", "status").alias("method_status_key"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled"),
    )
)

result = (
    cleaned
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.paid_at",
        "a.status",
        "a.paid_at_day",
        F.col("d.shipping_fee"),
        F.col("d.order_date"),
        F.col("d.status").alias("order_status"),
    )
)
