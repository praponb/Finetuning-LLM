from pyspark.sql import functions as F

dim_totals = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .groupingSets(
        [["status", "txn_type"], ["status"], []],
        F.col("p.status").alias("status"),
        F.col("e.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("e.amount")).alias("avg_amount"),
    )
)

result = (
    dim_totals
    .select("status", "txn_type", "avg_amount")
    .orderBy(F.col("txn_type").desc())
)
