from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("customer_id").isin([2, 7, 30]))
    .select("*", F.date_add("opened_date", 21).alias("opened_date_plus_21d"))
)

attributed = (
    cleaned
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.opened_date_plus_21d").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_dims = (
    attributed
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.age") >= 37)
    .select(
        "e.id",
        "e.customer_id",
        "e.account_type",
        "e.balance",
        "e.opened_date",
        "e.status",
        "e.opened_date_plus_21d",
        "e.subscription_id",
        "e.plan",
        "e.monthly_fee",
        F.col("p.city"),
    )
)

eligible = (
    with_dims
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") >= F.col("s.opened_date")), "left_anti")
)

daily = (
    eligible
    .groupBy(
        F.when(F.col("monthly_fee") >= 263, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_rows"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("premium_rows") > 1)
)

result = daily.select("monthly_fee_tier", "avg_monthly_fee")
