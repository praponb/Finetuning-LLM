from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter((F.col("monthly_fee") > 140) & (F.col("end_date") >= "2025-09-03"))
    .withColumn("status_upper", F.upper("status"))
)

subscriptions_stats = (
    filtered_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 268, "high").when(F.col("monthly_fee") >= 96, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
)

result = (
    subscriptions_stats
    .filter(F.col("monthly_fee_count") > 8)
    .select("monthly_fee_bucket", "enterprise_monthly_fee")
)
