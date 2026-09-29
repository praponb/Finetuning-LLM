from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"))
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
    .filter(F.col("stddev_monthly_fee") < 36)
)
