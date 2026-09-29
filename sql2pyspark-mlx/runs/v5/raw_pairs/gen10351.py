from pyspark.sql import functions as F

amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("Lazada"))))
    .select("*", F.date_trunc("month", "txn_time").alias("txn_time_period"))
)

with_dims = (
    filtered
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.amount",
        "s.merchant",
        "s.is_flagged",
        F.col("b.opened_date"),
        F.col("b.account_type"),
    )
)

kept_keys = (
    with_dims
    .select("merchant", "account_id")
    .intersectAll(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("merchant", "account_id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select("a.account_id", "a.opened_date", "a.account_type", F.col("b.balance"))
)

pivoted = (
    joined
    .groupBy("account_id")
    .pivot("account_type", ["SAVINGS", "LOAN"])
    .agg(F.max("balance"))
)

result = (
    pivoted
    .join(amount_by_account_id, on="account_id", how="left")
    .select("account_id", "SAVINGS", F.col("sum_transaction_amount"))
    .orderBy(
        F.col("account_id").desc_nulls_first(),
        F.col("sum_transaction_amount").desc(),
    )
    .limit(20)
)
