from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.balance") > 77000)
    .groupBy(F.col("a.id"), F.col("a.status"))
    .agg(F.count(F.col("t.id")).alias("transaction_count"))
    .orderBy("transaction_count")
)
