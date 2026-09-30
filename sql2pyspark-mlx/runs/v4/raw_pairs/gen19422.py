from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.merchant").isNotNull() & (F.col("a.balance") <= 46000))
    .select(F.col("t.id"), F.col("t.account_id"), F.col("a.status"))
)
