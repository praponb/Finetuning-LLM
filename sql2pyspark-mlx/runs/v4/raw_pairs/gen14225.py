from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .groupBy("status", "plan")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0))) / F.count("*")), 1).alias("pct_standard"),
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee")).otherwise(0)).alias("active_monthly_fee"),
    )
    .filter(F.col("pct_standard") > 26)
    .select("status", "plan", "pct_standard", "active_monthly_fee")
    .orderBy("plan")
)
