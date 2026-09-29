from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-03-01", "2023-09-01"))
    .groupBy("status")
    .pivot("plan", ["basic", "premium", "enterprise"])
    .agg(F.avg("monthly_fee"))
    .orderBy("status")
)
