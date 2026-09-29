from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count("*").alias("num_transactions"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_summary.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.num_transactions") >= 10)
    .select(
        F.col("accounts.balance"),
        F.col("t.num_transactions"),
        F.col("t.txn_type_list"),
    )
    .orderBy("num_transactions")
)
