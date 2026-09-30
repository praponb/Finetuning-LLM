from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-08-20").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.lower("account_type").alias("account_type_lower"),
        F.coalesce("status", F.lit("OTHER")).alias("status_clean"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.amount") < 2000), "left_anti")
)

with_window = (
    not_flagged
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.opened_date") >= F.col("sub.start_date")) & (F.col("src.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

eligible = (
    with_window
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date")), "left_anti")
)

valid_accounts = (
    eligible
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-09-28"))) & (F.col("customer_id") == 18))
    .select("*", F.trim("status").alias("clean_status"))
)

kept = (
    valid_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left_anti")
)

result = (
    kept
    .withColumn("subscription_id_tier", F.when(F.col("subscription_id") >= 10, "LARGE").otherwise("SMALL"))
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date"))
    .rollup("subscription_id", "subscription_id_tier", "week_start")
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("subscription_id")).otherwise(0)).alias("credit_subscription_id"),
        F.max(F.when(F.col("plan") == "premium", F.col("monthly_fee"))).alias("premium_max_monthly_fee"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("monthly_fee_per_enterprise"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.grouping_id().alias("grouping_level"),
    )
)
