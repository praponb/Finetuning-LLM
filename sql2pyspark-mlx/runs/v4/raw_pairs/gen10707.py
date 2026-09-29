from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.to_date(F.col("t.txn_time")) >= "2025-01-01")
    .groupBy(F.col("a.id"), F.col("a.status"))
    .agg(F.count(F.col("t.id")).alias("transaction_count"))
)
