from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("txn_type") == "FEE")
    .groupBy("account_id")
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("a.opened_date"),
        F.col("t.latest_txn_time"),
        F.col("t.merchant_list"),
    )
    .orderBy("latest_txn_time", "opened_date")
)
