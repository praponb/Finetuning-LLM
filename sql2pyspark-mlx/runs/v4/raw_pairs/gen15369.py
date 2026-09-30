from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("account_id").isin([1, 7, 10]))
    .groupBy("account_id")
    .agg(F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("t.txn_type_list"))
    .orderBy("opened_date")
)
