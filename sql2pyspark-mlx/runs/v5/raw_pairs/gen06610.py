from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") >= 1260)
    .drop("avg_amount")
    .orderBy("method")
    .limit(20)
)
