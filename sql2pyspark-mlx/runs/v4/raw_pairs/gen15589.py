from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") != "EXPIRED")
    .groupBy("plan")
    .pivot("status", ["ACTIVE", "EXPIRED"])
    .agg(F.count("monthly_fee"))
)
