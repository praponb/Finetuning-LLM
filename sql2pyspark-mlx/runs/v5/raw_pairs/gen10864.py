from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.to_date(F.lit("2023-05-29"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.upper("status").alias("status_upper"),
        F.dayofweek("paid_at").alias("paid_at_dayofweek"),
    )
)

result = (
    filtered
    .groupBy("order_id")
    .pivot("method", ["promptpay", "paypal"])
    .agg(F.sum("paid_at_dayofweek").alias("amount"))
)
