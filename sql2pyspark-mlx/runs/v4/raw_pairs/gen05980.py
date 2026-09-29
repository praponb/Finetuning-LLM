from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter((F.col("plan") == "enterprise") & (F.col("end_date") >= "2025-04-01"))
)

subscriptions_stats = (
    src
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
)

result = (
    subscriptions_stats
    .filter(F.col("monthly_fee_count") > 9)
    .orderBy(F.col("monthly_fee_count").desc())
)
