from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([20, 26, 35]) & (~F.col("plan").isin(["enterprise", "premium"]) | (F.col("plan") == "premium")))
    .groupBy("status")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0)).alias("enterprise_count"),
    )
    .filter(F.col("enterprise_count") > 71)
)
