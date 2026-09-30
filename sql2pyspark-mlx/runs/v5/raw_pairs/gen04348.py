from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("start_date") > F.to_date(F.lit("2023-06-18"))))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.floor("monthly_fee").alias("monthly_fee_whole"),
        F.when(F.col("plan") == "enterprise", 1).otherwise(0).alias("is_enterprise"),
    )
)

result = (
    valid
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.start_date") >= F.col("sub.start_date")) & (F.col("src.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)
