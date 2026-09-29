from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.upper("method").alias("method_upper"),
    )
)

result = (
    base
    .groupBy("order_id")
    .pivot("method", ["cash", "promptpay"])
    .agg(F.max("amount").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
