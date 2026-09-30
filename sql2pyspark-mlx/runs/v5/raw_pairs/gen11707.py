from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter(F.hour("paid_at").between(7, 11) & (F.col("paid_at") >= F.to_date(F.lit("2023-07-27"))))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.col("amount").cast("int").alias("amount_int"),
    )
)

result = (
    filtered
    .groupBy("order_id")
    .pivot("method", ["paypal", "cash", "bank_transfer"])
    .agg(F.sum("amount_int"))
)
