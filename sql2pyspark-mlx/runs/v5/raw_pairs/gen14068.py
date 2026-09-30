from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") >= 220) & ((F.col("plan") == "standard") | (F.col("status") == "PAUSED")))
    .groupBy("status", F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(
        F.max("start_date").alias("latest_start_date"),
        F.min("start_date").alias("first_start_date"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
    )
    .filter(F.col("cancelled_monthly_fee") >= 80)
    .drop("cancelled_monthly_fee")
    .orderBy("start_month", "first_start_date")
)
