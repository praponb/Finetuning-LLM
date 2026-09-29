from pyspark.sql import functions as F

matched_keys = (
    df_transactions
    .select("account_id")
    .intersectAll(
        df_transactions
        .filter(F.col("amount").between(1300, 3100))
        .select("account_id")
    )
)

kept_rows = (
    df_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "account_id",
        "txn_type",
        "amount",
        F.when(F.col("txn_type").rlike("^(CREDIT|FEE)$"), "GROUP_B").when(F.col("txn_type").rlike("^(DEBIT|REFUND)$"), "PRIORITY").otherwise("CORE").alias("txn_type_zone"),
    )
)
