from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("plan") == "standard"))
    .select("*", F.concat("plan", F.lit("/"), "status").alias("plan_status"))
)

enriched = (
    filtered
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.age"), F.col("c.signup_date"))
)

matched = (
    enriched
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.signup_date") >= F.col("sub.start_date")) & (F.col("src.signup_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

matched_subscriptions = (
    matched
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.end_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan_2"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee_2"),
    )
)

result = (
    matched_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "plan_status",
        "subscription_id",
        "subscription_plan",
    )
    .orderBy(F.col("id").desc(), F.col("subscription_id").desc())
)
