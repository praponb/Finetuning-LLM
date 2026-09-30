from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", "method")
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
    )
    .orderBy(F.col("failed_count").desc())
)
