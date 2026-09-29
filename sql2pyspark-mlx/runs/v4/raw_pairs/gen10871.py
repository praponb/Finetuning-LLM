from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter(F.col("paid_at") > F.to_timestamp(F.lit("2024-05-27 20:00:00")))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.hour("paid_at").alias("paid_at_hour"),
    )
)

result = (
    cleaned
    .groupBy("method")
    .pivot("status", ["SUCCESS", "REFUNDED"])
    .agg(F.sum("amount").alias("total"))
)
