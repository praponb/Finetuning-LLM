from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([3, 27]) & F.col("status").isin(["REFUNDED", "FAILED"]))
    .select(
        "id",
        "status",
        "amount",
        "paid_at",
        F.when(F.col("amount") >= 1250, "High").when(F.col("amount") >= 630, "Medium").otherwise("Low").alias("amount_tier"),
    )
)
