from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .groupBy("status")
    .agg(
        F.min("end_date").alias("min_end_date"),
        F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0)).alias("expired_count"),
    )
)
