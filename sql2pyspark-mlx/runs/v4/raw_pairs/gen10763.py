from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.txn_time") >= "2023-12-17")
    .groupBy(F.col("a.account_type"))
    .agg(F.min(F.col("t.amount")).alias("min_amount"))
)
