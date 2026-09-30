from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") > "2023-03-01")
    .groupBy("plan", F.quarter("start_date").alias("start_quarter"))
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.max("end_date").alias("latest_end_date"),
    )
)
