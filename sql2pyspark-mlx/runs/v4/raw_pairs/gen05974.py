from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .groupBy("status")
    .pivot("plan", ["enterprise", "basic"])
    .agg(F.count("monthly_fee"))
)
