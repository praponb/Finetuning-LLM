from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .pivot("plan", ["premium", "enterprise"])
    .agg(F.avg("monthly_fee").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
