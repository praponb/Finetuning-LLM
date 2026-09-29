from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("plan_length", F.length("plan"))
)

result = (
    filtered
    .groupBy("customer_id", "status")
    .pivot("plan", ["premium", "basic"])
    .agg(F.avg("monthly_fee"))
)
