from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .pivot("plan", ["standard", "premium"])
    .agg(F.sum("monthly_fee").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
