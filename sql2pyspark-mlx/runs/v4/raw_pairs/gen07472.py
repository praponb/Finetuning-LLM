from pyspark.sql import functions as F

amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("account_id") == 12))
    .select(
        "*",
        F.when(F.col("amount") >= 3500, "High").when(F.col("amount") >= 900, "Medium").otherwise("Low").alias("amount_tier"),
        F.lower("txn_type").alias("txn_type_lower"),
    )
)

valid_transactions = (
    valid
    .filter((F.col("txn_time") >= F.lit("2024-04-13 16:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "*",
        F.lower("merchant").alias("merchant_lower"),
        F.col("txn_time").cast("date").alias("txn_time_day"),
    )
)

with_dims = (
    valid_transactions
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.opened_date"), F.col("d.account_type"))
)

result = (
    with_dims
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "is_flagged",
        "amount_tier",
        "txn_time_day",
        "opened_date",
        F.when(F.col("txn_type").rlike("^(FEE|DEBIT)$"), "GROUP_B").when(F.col("txn_type").rlike("^(CREDIT|REFUND)$"), "EXTENDED").otherwise("CORE").alias("txn_type_zone"),
        F.col("sum_transaction_amount"),
    )
    .orderBy("txn_type_zone")
)
