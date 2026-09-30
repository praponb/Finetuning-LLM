from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", F.month("start_date").alias("start_month"))
    .agg(
        F.max("end_date").alias("latest_end_date"),
        F.max("start_date").alias("latest_start_date"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .orderBy("start_month")
)
