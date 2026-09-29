from pyspark.sql import functions as F

with_dims = (
    df_payments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.*",
        F.col("o.channel"),
        F.col("o.status").alias("order_status"),
        F.col("o.shipping_fee"),
    )
)

result = (
    with_dims
    .groupBy("method")
    .agg(
        F.min("paid_at").alias("first_seen"),
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_rows"),
    )
)
