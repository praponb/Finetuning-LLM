from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", "customer_id")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.max("end_date").alias("latest_end_date"),
    )
)
