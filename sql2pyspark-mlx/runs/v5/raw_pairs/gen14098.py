from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.concat_ws(",", "merchant", "txn_type").alias("merchant_txn_type_key"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

matched_keys = (
    base
    .select("merchant", "txn_type")
    .subtract(
        df_transactions
        .filter(F.col("amount") < 3200)
        .select("merchant", "txn_type")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("is_flagged"))
    .select("*", F.round("amount", 0).alias("amount_rounded"))
)

with_dims = (
    valid
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "left")
    .select(
        "e.account_id",
        "e.txn_type",
        "e.amount",
        "e.txn_time",
        "e.merchant_txn_type_key",
        "e.amount_rounded",
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("a.opened_date"),
    )
)

result = (
    with_dims
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["account_status", "account_account_type", "transaction_account_type"], ["account_status"]],
        F.col("p.status").alias("account_status"),
        F.col("p.account_type").alias("account_account_type"),
        F.col("src.account_type").alias("transaction_account_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.amount")).alias("max_amount"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("account_status").desc_nulls_first())
)
