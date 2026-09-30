from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "customer_id",
        F.when(F.col("monthly_fee") >= 184, "high").when(F.col("monthly_fee") >= 117, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
        F.min("start_date").alias("first_start_date"),
        F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0)).alias("expired_count"),
    )
)
