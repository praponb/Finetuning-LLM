from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id", "status")
    .pivot("plan", ["standard", "enterprise"])
    .agg(F.avg("monthly_fee").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
