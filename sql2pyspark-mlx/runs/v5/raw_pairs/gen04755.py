from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", "order_id", F.quarter("paid_at").alias("paid_quarter"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
    )
    .orderBy("status")
)
