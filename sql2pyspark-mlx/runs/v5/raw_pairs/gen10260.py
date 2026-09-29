from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-06-27").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("web"))))
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.lower("channel").alias("channel_lower"),
        F.floor("shipping_fee").alias("shipping_fee_whole"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") <= F.col("s.order_date")), "left_semi")
)
