from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter((F.col("plan") == "standard") & (F.col("end_date").between("2024-11-27", "2025-07-05") | (F.col("monthly_fee") <= 210)))
    .withColumn("start_date_plus_months", F.add_months("start_date", 6))
)

summary = (
    valid_subscriptions
    .groupBy(F.month("start_date_plus_months").alias("start_plus_months_month"))
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
    )
)

result = (
    summary
    .orderBy(F.col("min_monthly_fee").desc(), "start_plus_months_month")
    .limit(10)
)
