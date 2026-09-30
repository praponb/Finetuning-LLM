from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", "customer_id")
    .pivot("plan", ["enterprise", "standard"])
    .agg(F.avg("monthly_fee").alias("total"), F.count(F.lit(1)).alias("n"))
)
