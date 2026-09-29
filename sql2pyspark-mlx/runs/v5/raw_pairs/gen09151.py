from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") <= F.col("s.order_date")), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
    )
    .orderBy(F.col("amount").desc_nulls_last(), F.col("channel").desc())
)
