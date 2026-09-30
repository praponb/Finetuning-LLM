from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") != "REFUNDED") & (F.col("method") == "card"))
    .groupBy("method", F.to_date("paid_at").alias("paid_day"))
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("success_amount") < 62)
)
