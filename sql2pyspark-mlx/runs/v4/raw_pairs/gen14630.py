from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([1, 4, 7]) & (F.col("monthly_fee") <= 200))
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("status") == "PAUSED", F.col("monthly_fee")).otherwise(0)).alias("paused_monthly_fee"),
        F.max("end_date").alias("latest_end_date"),
    )
    .filter(F.col("paused_monthly_fee") < 51)
    .orderBy("latest_end_date", "plan")
    .limit(20)
)
