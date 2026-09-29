from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("status") == "ACTIVE"))
    .groupBy(
        F.date_format("end_date", "yyyy-MM").alias("end_month"),
        F.month("start_date").alias("start_month"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0)).alias("enterprise_count"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
)
