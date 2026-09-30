from pyspark.sql import functions as F

remaining_keys = (
    df_accounts
    .select("id", "status")
    .intersectAll(
        df_accounts
        .filter(F.col("balance") < 72000)
        .select("id", "status")
    )
)

reconciled = (
    df_accounts
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.abs("balance").alias("abs_balance"),
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date")), "left_anti")
)

joined = (
    not_flagged
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.email"), F.col("d.city"), F.col("d.signup_date"))
)

result = (
    joined
    .select(
        "id",
        "customer_id",
        "opened_date",
        "abs_balance",
        "balance_filled",
        "email",
        "city",
        F.when(F.col("city").rlike("^(Berlin|Singapore)$"), "GROUP_A").otherwise("CORE").alias("city_zone"),
    )
)
