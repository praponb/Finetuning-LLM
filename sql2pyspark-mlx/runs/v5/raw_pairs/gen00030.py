from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.round(((100 * F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_active"),
    )
    .orderBy(F.col("avg_monthly_fee").desc())
)
