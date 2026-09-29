from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 6)
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0))) / F.count("*")), 1).alias("pct_standard"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
)
