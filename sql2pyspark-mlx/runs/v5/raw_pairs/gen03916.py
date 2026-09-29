from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
    )
)
