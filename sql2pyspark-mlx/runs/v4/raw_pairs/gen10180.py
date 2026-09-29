from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") != "PAUSED")
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
    )
    .filter(F.col("avg_monthly_fee") >= 160)
    .select("end_quarter", "avg_monthly_fee", "enterprise_count")
)
