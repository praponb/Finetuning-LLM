from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("txn_type").isin(["REFUND", "DEBIT", "FEE"]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.round((F.col("amount") * 0.9), 2).alias("amount_scaled"),
    )
)

matched_keys = (
    cleaned
    .select("txn_type", "id")
    .intersect(
        df_transactions
        .filter(F.col("txn_time") >= "2024-06-01")
        .select("txn_type", "id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.balance"), F.col("d.opened_date"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "left")
    .filter(F.col("b.customer_id").isNotNull())
    .select(
        "src.account_id",
        "src.txn_type",
        "src.merchant",
        "src.is_flagged",
        F.col("b.balance").alias("account_balance"),
        F.col("b.account_type"),
        F.col("b.opened_date").alias("account_opened_date"),
    )
)

result = (
    with_dims
    .select(
        "account_id",
        "txn_type",
        "merchant",
        "is_flagged",
        "account_balance",
        "account_type",
        "account_opened_date",
        F.when(F.col("txn_type").rlike("^(FEE|DEBIT)$"), "CORE").when(F.col("txn_type").rlike("^(CREDIT|REFUND)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("txn_type_zone"),
    )
    .orderBy(F.col("account_type").desc_nulls_first())
)
