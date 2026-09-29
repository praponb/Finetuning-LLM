from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_expired"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .filter(F.col("pct_expired") < 56)
)
