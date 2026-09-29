from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["PAUSED", "CANCELLED"])
    .agg(F.sum("monthly_fee").alias("value_sum"))
)
