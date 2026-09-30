from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "ACTIVE")
    .groupBy("status")
    .pivot("plan", ["enterprise", "basic", "premium"])
    .agg(F.max("monthly_fee"))
)
