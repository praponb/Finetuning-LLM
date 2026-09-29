from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.balance").between(52000, 64000))
    .groupBy(F.col("a.status"), F.col("a.customer_id"))
    .agg(
        F.min(F.col("t.txn_time")).alias("first_txn_time"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").desc())
)
