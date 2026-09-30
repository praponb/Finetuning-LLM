from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.customer_id").isin([7, 24]))
    .groupBy(F.col("a.status"))
    .agg(F.count(F.col("t.id")).alias("transaction_count"))
    .orderBy("status", "transaction_count")
)
