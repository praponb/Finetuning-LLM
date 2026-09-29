from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.month("start_date").alias("start_month"))
    .pivot("status", ["CANCELLED", "ACTIVE", "EXPIRED"])
    .agg(F.max("monthly_fee"))
    .orderBy("start_month")
)
