from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 210) & ~F.col("plan").isin(["basic", "standard"]) & (F.col("status") == "EXPIRED"))
)

agg = (
    filtered_subscriptions
    .groupBy("plan", F.date_trunc("MONTH", "start_date").alias("start_month"))
    .agg(
        F.min("start_date").alias("first_start_date"),
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
    )
)

result = agg.select("plan", "first_start_date", "premium_monthly_fee")
