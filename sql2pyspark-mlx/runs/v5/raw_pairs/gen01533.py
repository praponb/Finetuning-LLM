from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "plan",
        F.when(F.col("plan").isin(["enterprise", "standard"]), "priority").otherwise("other").alias("plan_group"),
        "customer_id",
    )
    .agg(
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee")).otherwise(0)).alias("active_monthly_fee"),
    )
    .orderBy("plan", "plan_group", "customer_id")
    .limit(10)
)
