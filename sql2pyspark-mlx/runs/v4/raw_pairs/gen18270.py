from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 1070)
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0)).alias("promptpay_count"),
        F.min("paid_at").alias("min_paid_at"),
    )
    .orderBy("status")
)
