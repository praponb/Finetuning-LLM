from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~(~F.col("plan").isin(["enterprise", "basic", "premium"]) & (F.col("monthly_fee") < 80)))
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(
        F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("active_count"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
)
