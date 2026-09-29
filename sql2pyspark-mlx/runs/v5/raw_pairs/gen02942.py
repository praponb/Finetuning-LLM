from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .groupingSets(
        [["account_type", "txn_type"], ["account_type"], []],
        F.col("p.account_type").alias("account_type"),
        F.col("e.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("e.amount")).alias("min_amount"),
        F.grouping_id().alias("grouping_level"),
    )
)
