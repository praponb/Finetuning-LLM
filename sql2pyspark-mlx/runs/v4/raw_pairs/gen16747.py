from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "EXPIRED") | (F.col("customer_id") == 33))
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "PAUSED", 1).otherwise(0)).alias("paused_count"),
    )
)
