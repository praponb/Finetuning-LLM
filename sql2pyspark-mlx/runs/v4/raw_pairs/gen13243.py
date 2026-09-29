from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.max("start_date").alias("latest_start_date"),
    )
    .orderBy("latest_start_date")
)
