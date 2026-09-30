from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.hour(F.col("t.txn_time")).between(7, 15))
    .groupBy(F.col("a.id"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        (F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"),
    )
    .orderBy("amount_range")
)
