from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(transactions_by_account.alias("transactions_by_account"), F.col("accounts.id") == F.col("transactions_by_account.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.status"),
        F.col("transactions_by_account.txn_type_values"),
    )
    .orderBy("status", "id")
    .limit(10)
)
