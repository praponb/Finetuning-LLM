from pyspark.sql import functions as F

src = df_subscriptions.filter(F.col("plan") == "premium")

subscriptions_stats = (
    src
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"), "status")
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
)

result = (
    subscriptions_stats
    .filter(F.col("monthly_fee_range") < 79)
    .select("start_month", "status", "monthly_fee_range")
)
