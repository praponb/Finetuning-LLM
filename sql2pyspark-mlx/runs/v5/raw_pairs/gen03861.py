from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", F.quarter("end_date").alias("end_quarter"))
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.round(((100 * F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0))) / F.count("*")), 1).alias("pct_standard"),
    )
    .orderBy("pct_standard", "status", "end_quarter")
    .limit(5)
)
