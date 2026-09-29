from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") <= 1330)
    .groupBy("method", "status")
    .agg(
        F.count("*").alias("total_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0)).alias("bank_transfer_count"),
    )
    .filter(F.col("bank_transfer_count") > 59)
    .drop("bank_transfer_count")
    .orderBy("method", "status")
    .limit(3)
)
