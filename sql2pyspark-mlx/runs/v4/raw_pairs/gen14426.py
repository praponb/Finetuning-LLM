from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
)
