from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.max("amount").alias("max_amount"),
        F.countDistinct("amount").alias("unique_txn_types"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(transactions_by_account.alias("transactions_by_account"), F.col("accounts.id") == F.col("transactions_by_account.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.customer_id"),
        F.col("accounts.status"),
        F.col("transactions_by_account.max_amount"),
        F.col("transactions_by_account.unique_txn_types"),
    )
    .orderBy(F.col("unique_txn_types").desc())
)
