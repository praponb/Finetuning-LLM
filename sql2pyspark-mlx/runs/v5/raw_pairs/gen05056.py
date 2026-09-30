from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("subscription_id").desc())
w2 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

cleaned = (
    df_accounts
    .filter(F.col("opened_date") < "2025-05-01")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.trim("account_type").alias("clean_account_type"),
        F.coalesce("status", F.lit("NONE")).alias("resolved_status"),
    )
)

with_window = (
    cleaned
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.opened_date") >= F.col("sub.start_date")) & (F.col("a.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

ranked = (
    with_window
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance",
        "s.opened_date",
        "s.status",
        "s.resolved_status",
        "s.subscription_id",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.merchant"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "resolved_status",
        "subscription_id",
        "transaction_txn_time",
        "merchant",
        F.when(F.col("status").rlike("^(CLOSED|OPEN)$"), "GROUP_B").when(F.col("status").rlike("^(FROZEN)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy(F.col("id").asc_nulls_last(), "balance")
)
