from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.max("end_date").alias("last_end_date"),
    )
)
