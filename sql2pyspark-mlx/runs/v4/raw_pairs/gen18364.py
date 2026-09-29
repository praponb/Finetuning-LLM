from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-09-15").cast("date"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.round("amount", 2).alias("amount_rounded"),
    )
)

result = (
    cleaned
    .groupBy("order_id")
    .pivot("method", ["card", "paypal", "bank_transfer"])
    .agg(F.max("amount_rounded"))
)
