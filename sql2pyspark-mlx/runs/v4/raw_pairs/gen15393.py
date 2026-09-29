from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .groupBy(F.month("end_date").alias("end_month"))
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
    .filter(F.col("max_monthly_fee") >= 110)
    .orderBy(F.col("end_month").desc())
)
