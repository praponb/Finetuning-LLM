from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([3, 5, 7]))
    .groupBy("status")
    .pivot("plan", ["standard", "enterprise", "premium", "basic"])
    .agg(F.count("monthly_fee"))
)
