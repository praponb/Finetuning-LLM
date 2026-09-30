from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["EXPIRED", "PAUSED", "ACTIVE", "CANCELLED"])
    .agg(F.max("monthly_fee"))
)
