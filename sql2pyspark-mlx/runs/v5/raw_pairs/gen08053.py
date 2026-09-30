from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.month("start_date").isin([9, 11, 12]))
    .withColumn("start_date_minus_60d", F.date_sub("start_date", 60))
)

subscriptions_stats = (
    recent_subscriptions
    .groupBy(
        F.year("start_date").alias("start_year"),
        F.when(F.col("status").isin(["CANCELLED", "PAUSED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.countDistinct("monthly_fee").alias("unique_plans"),
    )
)

result = (
    subscriptions_stats
    .filter(F.col("avg_monthly_fee") < 80)
    .select("start_year", "status_group", "avg_monthly_fee", "unique_plans")
    .orderBy(
        F.col("avg_monthly_fee").desc(),
        F.col("unique_plans").desc(),
        "start_year",
        "status_group",
    )
    .limit(50)
)
