from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 8)
    .groupBy("status")
    .pivot("plan", ["enterprise", "standard", "basic", "premium"])
    .agg(F.count("monthly_fee"))
)
