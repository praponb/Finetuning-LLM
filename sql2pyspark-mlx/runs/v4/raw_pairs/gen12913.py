from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .groupBy(F.col("a.account_type"))
    .agg(F.count(F.col("t.amount")).alias("amount_count"))
    .orderBy(F.col("account_type").desc())
)
