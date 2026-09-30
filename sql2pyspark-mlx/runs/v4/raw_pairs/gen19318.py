from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNotNull() & (F.col("monthly_fee") < 170) & (F.col("plan") == "premium"))
    .groupBy("plan", "status")
    .agg(
        F.sum(F.when(F.col("plan") == "standard", F.col("monthly_fee")).otherwise(0)).alias("standard_monthly_fee"),
        F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0)).alias("expired_count"),
    )
    .orderBy("plan")
)
