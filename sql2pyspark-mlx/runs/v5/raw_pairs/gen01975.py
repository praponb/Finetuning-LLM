from pyspark.sql import functions as F

valid_subscriptions = df_subscriptions.filter(F.col("start_date") < "2024-12-01")

status_summary = (
    valid_subscriptions
    .groupBy("status")
    .agg(
        F.max("start_date").alias("latest_start_date"),
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
    )
)

result = status_summary.filter(F.col("premium_monthly_fee") > 58)
