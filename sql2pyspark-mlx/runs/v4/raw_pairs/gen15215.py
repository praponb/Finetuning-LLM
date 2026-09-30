from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        F.when(F.col("plan") == "basic", 1).otherwise(0).alias("is_basic"),
    )
)

result = (
    base
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.end_date") >= F.col("sub.start_date")) & (F.col("src.end_date") <= F.date_add(F.col("sub.end_date"), 3)), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)
