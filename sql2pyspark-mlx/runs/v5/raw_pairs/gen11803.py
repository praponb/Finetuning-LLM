from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-11-08 01:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.length("method").alias("method_length"),
        F.coalesce("method", F.lit("OTHER")).alias("method_clean"),
    )
)

enriched = (
    base
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.shipping_fee"),
        F.col("d.amount").alias("order_amount"),
        F.col("d.discount"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .filter(~F.col("o.channel").isin(["partner", "mobile"]))
    .select(
        "e.*",
        F.col("o.shipping_fee").alias("order_shipping_fee"),
        F.col("o.channel"),
    )
)

valid = (
    with_dims
    .filter((F.col("paid_at") >= F.to_date(F.lit("2023-05-19"))) & F.col("order_id").isNotNull())
    .select(
        "*",
        F.round("order_shipping_fee", 0).alias("order_shipping_fee_rounded"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.col("o.status").alias("status"),
        F.col("e.channel").alias("payment_channel"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.amount")).alias("max_amount"),
        F.round(F.sum(F.col("e.order_shipping_fee")), 2).alias("total_order_shipping_fee"),
    )
    .orderBy(F.col("total_order_shipping_fee").asc_nulls_first())
)
