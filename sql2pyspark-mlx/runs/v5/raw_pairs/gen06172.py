from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-08-10").cast("date")) & (F.year("opened_date") >= 2024))
    .withColumn("abs_balance", F.abs("balance"))
    .withColumn("account_type_status", F.concat("account_type", F.lit(", "), "status"))
    .withColumn("status_filled", F.coalesce("status", F.lit("DIRECT")))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

filtered_accounts = (
    with_latest
    .filter((F.col("opened_date") > F.lit("2023-09-20").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "account_type_status",
        "transaction_txn_time",
        F.initcap(F.trim("transaction_txn_type")).alias("transaction_txn_type_title"),
        F.coalesce("transaction_txn_type", F.lit("DIRECT")).alias("transaction_txn_type_filled"),
    )
)

with_dims = (
    filtered_accounts
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.segment"))
)

matched = (
    with_dims
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.transaction_txn_time") >= F.col("sub.start_date")) & (F.col("src.transaction_txn_time") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

kept_keys = (
    matched
    .select("customer_id", "status")
    .intersectAll(
        df_accounts
        .filter(F.col("account_type").isin(["SAVINGS", "CREDIT", "CHECKING"]))
        .select("customer_id", "status")
    )
)

reconciled = (
    matched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_semi")
)

result = (
    eligible
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        "transaction_txn_time",
        "transaction_txn_type_filled",
        "segment",
        "subscription_id",
        "plan",
        F.col("avg_account_balance"),
    )
)
