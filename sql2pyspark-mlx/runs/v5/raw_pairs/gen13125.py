from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
    )
    .filter(F.col("enterprise_monthly_fee") >= 60)
    .select("customer_id", "enterprise_monthly_fee", "active_count")
)
