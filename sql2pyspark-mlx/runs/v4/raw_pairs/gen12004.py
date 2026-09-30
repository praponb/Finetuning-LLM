from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["EXPIRED", "PAUSED", "CANCELLED", "ACTIVE"])
    .agg(F.sum("monthly_fee"))
)
