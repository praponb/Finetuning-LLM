from pyspark.sql import functions as F

amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

remaining_keys = (
    df_transactions
    .select("id")
    .intersect(
        df_transactions
        .filter(F.hour("txn_time").between(9, 17))
        .select("id")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        F.when(F.col("merchant").rlike("^(Lazada|Amazon)$"), "GROUP_A").otherwise("PRIORITY").alias("merchant_zone"),
        F.col("sum_transaction_amount"),
    )
)
