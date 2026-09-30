from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([1, 8, 10]) & F.col("customer_id").isin([1, 12]))
    .groupBy(
        "customer_id",
        "plan",
        F.when(F.col("monthly_fee") >= 216, "high").when(F.col("monthly_fee") >= 40, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("premium_count").desc())
)
