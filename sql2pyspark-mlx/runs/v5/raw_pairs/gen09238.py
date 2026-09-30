from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["ACTIVE", "CANCELLED"])
    .agg(F.sum("monthly_fee"))
    .orderBy("plan")
)
