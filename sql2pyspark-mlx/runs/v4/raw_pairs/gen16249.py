from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", "status", "order_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
    )
    .orderBy(F.col("method").desc(), "status", "order_id")
    .limit(5)
)
