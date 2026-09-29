from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.max("end_date").alias("latest_end_date"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
)
