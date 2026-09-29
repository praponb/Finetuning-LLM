from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", "status")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0)).alias("expired_count"),
    )
    .filter(F.col("max_monthly_fee") > 140)
    .orderBy("expired_count", "plan", "status")
    .limit(5)
)
