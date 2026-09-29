from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .groupBy(F.col("a.id"), F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"),
        F.sum(F.when(F.col("t.merchant") == "Lazada", F.col("t.amount")).otherwise(0)).alias("lazada_amount"),
    )
    .filter(F.col("avg_amount") > 42)
)
