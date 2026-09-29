from pyspark.sql import functions as F

recent_subscriptions = df_subscriptions.filter(F.month("end_date").isin([2, 5, 8]))

summary = (
    recent_subscriptions
    .groupBy("status")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
)

result = (
    summary
    .select("status", "monthly_fee_range")
    .orderBy(F.col("monthly_fee_range").desc(), "status")
)
