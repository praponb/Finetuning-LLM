from pyspark.sql import functions as F

recent_subscriptions = df_subscriptions.filter(F.col("customer_id") == 6)

plan_summary = (
    recent_subscriptions
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
    )
)

result = (
    plan_summary
    .filter(F.col("enterprise_monthly_fee") < 49)
    .select("plan", "enterprise_monthly_fee")
    .orderBy("plan")
)
