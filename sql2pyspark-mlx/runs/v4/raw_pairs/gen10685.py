from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["PAUSED", "CANCELLED", "ACTIVE", "EXPIRED"])
    .agg(F.count("monthly_fee"))
    .orderBy("plan")
)
