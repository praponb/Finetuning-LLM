from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(100, 230))
    .groupBy(
        F.when(F.col("status").isin(["ACTIVE", "CANCELLED"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
    )
    .filter(F.col("enterprise_monthly_fee") < 45)
)
