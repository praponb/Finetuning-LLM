from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.account_id") == 33)
    .select(F.col("t.id"), F.col("t.merchant"), F.col("t.txn_type"), F.col("a.balance"))
    .orderBy(F.col("balance").desc())
)
