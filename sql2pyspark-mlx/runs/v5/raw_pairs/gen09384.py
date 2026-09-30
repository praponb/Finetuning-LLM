from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(~F.col("txn_type").isin(["CREDIT", "DEBIT", "FEE"]))
    .groupBy("account_id")
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .select(
        F.col("a.opened_date"),
        F.col("as.last_txn_time"),
        F.col("as.merchant_list"),
    )
    .orderBy("opened_date")
)
