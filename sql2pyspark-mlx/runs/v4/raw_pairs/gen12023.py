from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("status") != "PAUSED") & (F.col("plan").isin(["premium", "basic", "standard"]) | F.col("status").isin(["CANCELLED", "PAUSED"])))
)

subscriptions_stats = (
    base
    .groupBy("plan", F.date_trunc("MONTH", "end_date").alias("end_month"))
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
)

result = (
    subscriptions_stats
    .filter(F.col("stddev_monthly_fee") < 30)
    .select("plan", "end_month", "avg_monthly_fee")
    .orderBy("end_month", F.col("avg_monthly_fee").desc(), "plan")
    .limit(50)
)
