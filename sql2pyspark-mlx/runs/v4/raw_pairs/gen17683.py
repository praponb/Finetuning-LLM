from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
)
