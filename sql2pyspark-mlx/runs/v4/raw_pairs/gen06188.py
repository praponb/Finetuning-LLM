from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "basic"]) & F.col("end_date").between("2023-05-01", "2025-01-04"))
)

summary = (
    recent_subscriptions
    .groupBy("status")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("active_count"),
    )
)

result = (
    summary
    .filter(F.col("active_count") >= 29)
    .orderBy(F.col("active_count").desc(), "status")
)
