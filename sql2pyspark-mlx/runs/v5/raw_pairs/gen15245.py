from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("account_type") != "CHECKING").alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .filter(F.col("t.account_id").isin([6, 14, 22, 23]))
    .select(F.col("t.id"), F.col("t.txn_time"))
    .orderBy("id", F.col("txn_time").desc())
)
