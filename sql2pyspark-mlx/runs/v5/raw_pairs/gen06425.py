from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("dim"), F.col("src.account_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["status", "account_type", "txn_type"], ["status", "account_type"], ["status"]],
        F.col("dim.status").alias("status"),
        F.col("dim.account_type").alias("account_type"),
        F.col("src.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.amount")).alias("max_amount"),
        F.grouping_id().alias("gid"),
    )
)
