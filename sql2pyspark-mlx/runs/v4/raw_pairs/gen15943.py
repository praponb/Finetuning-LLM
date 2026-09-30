from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("start_date") > "2024-01-11"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.last_day("start_date").alias("start_date_month_end"),
    )
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.city"), F.col("c.name"), F.col("c.segment"))
)

enriched = (
    with_dims
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.signup_date"))
)

remaining_keys = (
    enriched
    .select("plan", "id")
    .subtract(
        df_subscriptions
        .filter(F.col("plan").isin(["premium", "standard", "enterprise"]))
        .select("plan", "id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .rollup(F.date_trunc("MONTH", "start_date_month_end").alias("activity_month"))
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
        F.sum(F.when(F.col("segment") == "Consumer", F.col("monthly_fee")).otherwise(0)).alias("consumer_monthly_fee"),
    )
    .filter(F.col("event_count") > 1)
    .orderBy(F.col("event_count").desc_nulls_first())
)
