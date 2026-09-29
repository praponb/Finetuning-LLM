from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .groupingSets(
        [["account_type", "status", "txn_type"], ["account_type"], []],
        F.col("p.account_type").alias("account_type"),
        F.col("p.status").alias("status"),
        F.col("s.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("s.amount")), 2).alias("total_amount"),
    )
    .orderBy(F.col("cnt").desc_nulls_first())
)
