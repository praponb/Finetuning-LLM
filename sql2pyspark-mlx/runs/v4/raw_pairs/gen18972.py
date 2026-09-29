from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("status") == "SUCCESS") & (F.col("order_id") == 33)) | F.col("method").isin(["card", "promptpay", "cash"]))
    .groupBy(
        F.when(F.col("amount") >= 1270, "high").when(F.col("amount") >= 760, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.avg("amount").alias("avg_amount"),
    )
)
