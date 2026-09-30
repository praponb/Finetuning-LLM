from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", F.quarter("start_date").alias("start_quarter"))
    .agg(
        F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0)).alias("standard_count"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .filter(F.col("standard_count") > 30)
    .select("status", "start_quarter", "standard_count", "min_monthly_fee")
)
