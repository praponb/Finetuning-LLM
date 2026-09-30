from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") != "SUCCESS") & ~F.col("method").isin(["cash", "promptpay"]) & (F.col("order_id") == 21))
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
        F.min("paid_at").alias("min_paid_at"),
    )
)
