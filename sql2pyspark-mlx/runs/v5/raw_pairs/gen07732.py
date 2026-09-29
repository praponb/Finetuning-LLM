from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.merchant").isin(["Grab", "Amazon", "Lazada"]))
    .groupBy(F.col("a.id"))
    .agg(F.count(F.col("t.id")).alias("transaction_count"))
    .filter(F.col("transaction_count") > 3)
    .orderBy("transaction_count", F.col("id").desc())
    .limit(3)
)
