from pyspark.sql import functions as F

amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

filtered = (
    df_transactions
    .filter((F.to_date("txn_time") < "2023-11-20") & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.coalesce("merchant", F.lit("N/A")).alias("resolved_merchant"),
    )
)

cleaned = (
    filtered
    .filter((F.col("txn_time") > F.lit("2023-04-29").cast("date")) & (F.col("txn_type") == "DEBIT"))
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_flagged_label",
        "resolved_merchant",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label_2"),
        F.when(F.col("txn_type") == "REFUND", 1).otherwise(0).alias("is_refund"),
    )
)

matched_keys = (
    cleaned
    .select("account_id")
    .exceptAll(
        df_transactions
        .filter(F.col("txn_type") != "DEBIT")
        .select("account_id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    kept_rows
    .select("merchant", "account_id")
    .intersectAll(
        df_transactions
        .filter(F.col("amount") >= 1600)
        .select("merchant", "account_id")
    )
)

kept_rows_transactions = (
    kept_rows
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows_transactions
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "account_id",
        "is_flagged",
        "is_flagged_label",
        "resolved_merchant",
        "is_flagged_label_2",
        "is_refund",
        F.col("sum_transaction_amount"),
    )
    .orderBy(F.col("sum_transaction_amount").desc())
)
