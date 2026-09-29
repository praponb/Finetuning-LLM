from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
    .orderBy(F.col("avg_monthly_fee").desc(), "status")
    .limit(5)
)
