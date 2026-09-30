from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "EXPIRED") | (F.col("start_date") < "2023-03-31"))
    .groupBy("plan")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
    .orderBy(F.col("avg_monthly_fee").desc(), "plan")
    .limit(3)
)
