from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.customer_id").isNull() & (F.col("a.customer_id") == 37))
    .select(F.col("a.status"), F.col("t.is_flagged"), F.col("t.txn_time"))
    .orderBy("status", "txn_time")
)
