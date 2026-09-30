from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") > "2025-08-01")
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"), "status")
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
    .orderBy(F.col("monthly_fee_range").desc())
)
