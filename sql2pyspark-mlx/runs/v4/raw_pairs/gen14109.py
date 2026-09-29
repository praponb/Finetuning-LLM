from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 252, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
        F.max(F.when(F.col("plan") == "premium", F.col("monthly_fee"))).alias("premium_max_monthly_fee"),
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_rows"),
    )
    .orderBy("paused_rows")
)
