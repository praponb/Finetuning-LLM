from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter((F.col("end_date") > "2023-12-01") & (F.col("plan") == "enterprise") & (F.col("customer_id") == 5))
)

subscriptions_stats = (
    recent_subscriptions
    .groupBy(
        "plan",
        F.when(F.col("monthly_fee") >= 230, "high").when(F.col("monthly_fee") >= 63, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
)

result = subscriptions_stats.filter(F.col("max_monthly_fee") < 180)
