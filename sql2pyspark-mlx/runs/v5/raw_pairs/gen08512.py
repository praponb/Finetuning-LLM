from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["CANCELLED", "EXPIRED", "ACTIVE"])
    .agg(F.max("monthly_fee").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
