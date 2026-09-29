from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .pivot("status", ["CANCELLED", "PAUSED", "EXPIRED"])
    .agg(F.sum("monthly_fee").alias("total"))
)
