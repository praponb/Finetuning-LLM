from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", "customer_id")
    .pivot("plan", ["premium", "standard"])
    .agg(F.sum("monthly_fee"))
)
