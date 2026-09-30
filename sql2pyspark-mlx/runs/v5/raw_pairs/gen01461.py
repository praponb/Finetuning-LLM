from pyspark.sql import functions as F

kept_keys = (
    df_accounts
    .select("customer_id")
    .exceptAll(
        df_accounts
        .filter(F.col("opened_date") <= "2024-09-01")
        .select("customer_id")
    )
)

reconciled = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

remaining_keys = (
    reconciled
    .select("status", "account_type")
    .subtract(
        df_accounts
        .filter(F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]))
        .select("status", "account_type")
    )
)

kept_rows = (
    reconciled
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("status").isNotNull() & (F.col("opened_date") >= "2024-04-01"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.lower("account_type").alias("account_type_lower"),
        F.trim("account_type").alias("clean_account_type"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date")), "left_anti")
)

result = (
    kept
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["country", "account_type"], ["country"]],
        F.col("dim.country").alias("country"),
        F.col("e.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("e.balance")).alias("avg_balance"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("account_type").desc_nulls_last())
)
