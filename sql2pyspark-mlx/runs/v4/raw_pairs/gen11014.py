from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1410, "high").when(F.col("amount") >= 310, "mid").otherwise("low").alias("amount_bucket"),
        F.hour("paid_at").alias("paid_hour"),
    )
    .agg(F.count("*").alias("payment_count"))
)
