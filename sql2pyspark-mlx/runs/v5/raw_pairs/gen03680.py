from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"), "status")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .orderBy(F.col("avg_monthly_fee").desc(), "status")
)
