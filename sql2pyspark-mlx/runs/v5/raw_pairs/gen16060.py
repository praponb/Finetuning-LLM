from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.max("end_date").alias("last_end_date"),
    )
    .orderBy("last_end_date", F.col("monthly_fee_count").desc())
)
