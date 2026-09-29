from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("opened_date") <= "2024-07-01").alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .filter(F.col("t.is_flagged"))
    .select(F.col("t.id"), F.col("t.txn_time"), F.col("t.is_flagged"))
    .orderBy("txn_time", "id")
)
