from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") <= "2025-03-22")
    .agg(
        F.max("end_date").alias("last_end_date"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .orderBy("last_end_date")
)
