from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["paypal", "bank_transfer"]) & (F.col("method") != "bank_transfer") & (F.col("status") != "FAILED"))
    .groupBy(
        F.when(F.col("amount") >= 1590, "high").when(F.col("amount") >= 430, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .orderBy("amount_bucket")
)
