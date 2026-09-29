from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "cash", "bank_transfer"]))
    .groupBy("order_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .orderBy("first_paid_at", "order_id")
    .limit(5)
)
