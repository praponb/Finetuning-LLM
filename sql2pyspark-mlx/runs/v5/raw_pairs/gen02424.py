from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "EXPIRED")
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0)).alias("expired_count"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
    .filter(F.col("monthly_fee_count") < 5)
    .select("plan", "expired_count", "monthly_fee_count")
    .orderBy(F.col("expired_count").desc(), "plan")
)
