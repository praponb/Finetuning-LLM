from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-03-21").cast("date"))
    .select(
        "*",
        F.floor("amount").alias("amount_whole"),
        F.ceil("shipping_fee").alias("shipping_fee_whole"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") >= F.col("s.order_date")), "left_semi")
)
