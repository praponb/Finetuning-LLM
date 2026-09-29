from pyspark.sql import functions as F

result = (
    df_payments
    .rollup("status", "method")
    .agg(
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
        F.count("amount").alias("amount_count"),
    )
    .orderBy("status")
)
