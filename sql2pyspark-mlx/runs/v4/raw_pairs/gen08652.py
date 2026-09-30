from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.status").isin(["OPEN", "FROZEN"]))
    .groupBy(F.col("a.id"))
    .agg(F.count(F.col("t.id")).alias("transaction_count"))
)
