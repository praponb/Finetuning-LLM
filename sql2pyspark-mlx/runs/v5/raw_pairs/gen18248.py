from pyspark.sql import functions as F

result = (
    df_subscriptions
    .rollup(
        F.date_trunc("MONTH", "end_date").alias("activity_month"),
        F.date_trunc("YEAR", "start_date").alias("year_start"),
    )
    .agg(
        F.avg(F.when(F.col("status") == "PAUSED", F.col("monthly_fee"))).alias("paused_avg_monthly_fee"),
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_count"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_rows"),
    )
    .orderBy(F.col("premium_rows").desc())
)
