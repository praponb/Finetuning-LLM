from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("end_date") >= F.to_date(F.lit("2024-03-14")))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.trim("status").alias("clean_status"),
    )
)

attributed = (
    base
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.start_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

base_subscriptions = (
    attributed
    .filter((F.col("start_date") > F.lit("2024-01-31").cast("date")) & F.col("subscription_plan").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "clean_status",
        "subscription_id",
        "subscription_monthly_fee",
        F.upper("plan").alias("plan_upper"),
        F.add_months("start_date", 3).alias("start_date_plus_months"),
    )
)

result = (
    base_subscriptions
    .groupBy("customer_id")
    .pivot("status", ["EXPIRED", "PAUSED", "CANCELLED"])
    .agg(F.avg("subscription_monthly_fee"))
)
