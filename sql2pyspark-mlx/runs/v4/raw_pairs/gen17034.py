from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.month("end_date").alias("end_month"))
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
    .filter(F.col("min_monthly_fee") >= 180)
)
