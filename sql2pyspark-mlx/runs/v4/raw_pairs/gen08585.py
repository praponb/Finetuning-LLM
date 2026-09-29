from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 530)
    .groupBy(F.to_date("paid_at").alias("paid_day"))
    .agg(
        F.sum(F.when(F.col("method") == "promptpay", F.col("amount")).otherwise(0)).alias("promptpay_amount"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
    )
)
