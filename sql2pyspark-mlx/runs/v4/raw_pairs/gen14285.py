from pyspark.sql import functions as F

src = df_subscriptions.filter(F.col("start_date") < "2024-08-08")

subscriptions_stats = (
    src
    .groupBy("plan")
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
)

result = (
    subscriptions_stats
    .select("plan", "total_monthly_fee")
    .orderBy("plan")
)
