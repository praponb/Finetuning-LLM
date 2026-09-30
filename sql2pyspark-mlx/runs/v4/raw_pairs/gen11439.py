from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy(F.col("opened_date").desc_nulls_last())
w2 = Window.partitionBy("account_type", "plan").orderBy(F.col("opened_date").asc_nulls_first())
w3 = Window.partitionBy("status").orderBy(F.col("subscription_id").asc_nulls_last(), "id", "subscription_id")
w4 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

attributed = (
    df_accounts
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.opened_date") >= F.col("sub.start_date")) & (F.col("a.opened_date") <= F.date_add(F.col("sub.end_date"), 7)), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

ranked = (
    attributed
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

deduped = (
    ranked
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped_accounts = (
    deduped
    .withColumn("row_num_2", F.row_number().over(w3))
    .filter(F.col("row_num_2") == 1)
    .drop("row_num_2")
)

with_latest = (
    deduped_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

remaining_keys = (
    with_latest
    .select("customer_id", "account_type")
    .intersect(
        df_accounts
        .filter(F.col("status").isin(["FROZEN", "CLOSED", "OPEN"]))
        .select("customer_id", "account_type")
    )
)

reconciled = (
    with_latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.email"), F.col("c.credit_limit"), F.col("c.country"))
)

result = (
    enriched
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "opened_date",
        "plan",
        "monthly_fee",
        "transaction_txn_time",
        "transaction_amount",
        "credit_limit",
        F.when(F.col("plan").rlike("^(premium|basic)$"), "GROUP_B").when(F.col("plan").rlike("^(standard|enterprise)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("plan_zone"),
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(F.col("opened_date").asc_nulls_last())
)
