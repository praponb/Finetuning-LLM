from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-06-03").cast("date"))
    .select(
        "*",
        F.round("balance", 1).alias("balance_rounded"),
        F.coalesce("status", F.lit("N/A")).alias("status_clean"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .filter(~F.col("c.is_active"))
    .select("e.*", F.col("c.age"))
)

enriched_accounts = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.city"), F.col("p.segment"))
)

with_window = (
    enriched_accounts
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.opened_date") >= F.col("sub.start_date")) & (F.col("src.opened_date") <= F.date_add(F.col("sub.end_date"), 3)), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    with_window
    .groupBy(
        F.when(F.col("balance") >= 87000, "LARGE").otherwise("SMALL").alias("balance_tier"),
        "customer_id",
    )
    .agg(
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
        F.countDistinct("city").alias("distinct_cities"),
        F.max("balance").alias("max_balance"),
        F.round(F.sum("subscription_id") / F.nullif(F.count(F.when(F.col("segment") == "Consumer", 1)), F.lit(0)), 2).alias("subscription_id_per_consumer"),
        F.round(F.avg("subscription_id"), 2).alias("avg_subscription_id"),
        F.round(F.sum("balance_rounded") / F.nullif(F.count(F.when(F.col("account_type") == "LOAN", 1)), F.lit(0)), 2).alias("balance_rounded_per_loan"),
    )
    .orderBy(F.col("balance_rounded_per_loan").desc_nulls_last())
)
