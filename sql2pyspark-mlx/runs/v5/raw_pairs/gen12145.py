from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("status") == "EXPIRED")
    .withColumn("status_upper", F.upper("status"))
)

subscriptions_stats = (
    src
    .groupBy(F.month("start_date").alias("start_month"))
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0)).alias("enterprise_count"),
    )
)

result = (
    subscriptions_stats
    .select("start_month", "avg_monthly_fee", "enterprise_count")
    .orderBy(F.col("enterprise_count").desc(), "start_month")
    .limit(20)
)
