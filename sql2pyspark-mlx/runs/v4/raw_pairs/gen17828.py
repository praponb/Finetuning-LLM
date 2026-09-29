from pyspark.sql import functions as F

result = (
    df_subscriptions
    .withColumn("monthly_fee_tier", F.when(F.col("monthly_fee") >= 208, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("MONTH", "end_date"))
    .cube("monthly_fee_tier", "activity_month")
    .agg(
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_rows"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("activity_month").asc_nulls_last())
)
