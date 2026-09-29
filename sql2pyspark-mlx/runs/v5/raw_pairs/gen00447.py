from pyspark.sql import functions as F

valid_subscriptions = df_subscriptions.filter(F.col("monthly_fee") >= 70)

summary = (
    valid_subscriptions
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee")).otherwise(0)).alias("expired_monthly_fee"),
    )
)

result = summary.filter(F.col("expired_monthly_fee") < 36)
