from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(F.col("status") == "EXPIRED")
    .withColumn("monthly_fee_whole", F.floor("monthly_fee"))
)

subscriptions_stats = (
    filtered_subscriptions
    .groupBy("plan", F.quarter("end_date").alias("end_quarter"))
    .agg(
        F.count("monthly_fee_whole").alias("monthly_fee_whole_count"),
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee_whole")).otherwise(0)).alias("expired_monthly_fee_whole"),
    )
)

result = (
    subscriptions_stats
    .select("plan", "end_quarter", "expired_monthly_fee_whole")
    .orderBy(F.col("end_quarter").desc(), "plan")
)
