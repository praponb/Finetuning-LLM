from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .orderBy(F.col("stddev_monthly_fee").desc())
)
