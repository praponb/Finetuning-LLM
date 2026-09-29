from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") < "2025-04-01")
    .groupBy("status")
    .pivot("plan", ["standard", "basic", "premium"])
    .agg(F.count("monthly_fee"))
    .orderBy("status")
)
