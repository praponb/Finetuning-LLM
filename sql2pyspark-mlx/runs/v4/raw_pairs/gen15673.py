from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.count("*").alias("total_count"),
    )
)
