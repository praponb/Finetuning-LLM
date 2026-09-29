from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 180)
    .withColumn("start_date_label", F.date_format("start_date", "yyyy-MM-dd"))
)

subscriptions_stats = (
    filtered_subscriptions
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
)

result = (
    subscriptions_stats
    .select("end_month", "total_monthly_fee", "monthly_fee_count")
    .orderBy("end_month")
)
