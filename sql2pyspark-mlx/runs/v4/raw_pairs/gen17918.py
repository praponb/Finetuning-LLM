from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & F.col("plan").isin(["basic", "enterprise"]))
    .select("*", F.floor("monthly_fee").alias("monthly_fee_whole"))
)

with_window = (
    cleaned
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.end_date") >= F.col("sub.start_date")) & (F.col("e.end_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

joined = (
    with_window
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.age"), F.col("b.segment"), F.col("b.country"))
)

result = (
    joined
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "monthly_fee_whole",
        "subscription_id",
        F.when(F.col("status").rlike("^(CANCELLED|PAUSED)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy("monthly_fee_whole")
)
