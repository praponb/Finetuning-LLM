from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "customer_id",
        F.when(F.col("monthly_fee") >= 182, "high").when(F.col("monthly_fee") >= 77, "mid").otherwise("low").alias("monthly_fee_bucket"),
        "status",
    )
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
    .filter(F.col("avg_monthly_fee") >= 27)
    .orderBy(F.col("avg_monthly_fee").desc())
)
