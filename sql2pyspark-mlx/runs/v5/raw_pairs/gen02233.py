from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 225, "high").when(F.col("monthly_fee") >= 83, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee")).otherwise(0)).alias("active_monthly_fee"),
    )
)
