from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.month(F.col("a.opened_date")).isin([3, 9, 11]))
    .groupBy(F.col("a.customer_id"))
    .agg(
        F.max(F.col("t.txn_time")).alias("latest_txn_time"),
        F.sort_array(F.collect_set(F.col("t.merchant"))).alias("merchant_list"),
    )
)
