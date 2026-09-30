from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.min("amount").alias("min_amount"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
        F.count("amount").alias("amount_count"),
    )
    .orderBy("amount_count")
)
