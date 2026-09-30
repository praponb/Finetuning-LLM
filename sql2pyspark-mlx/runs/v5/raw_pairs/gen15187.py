from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["enterprise", "standard"])
    .agg(F.max("monthly_fee"))
)
