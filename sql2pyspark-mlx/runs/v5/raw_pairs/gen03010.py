from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.last_day("start_date").alias("start_date_month_end"),
    )
)

result = (
    base
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.start_date") >= F.col("sub.start_date")) & (F.col("a.start_date") <= F.date_add(F.col("sub.end_date"), 7)), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)
