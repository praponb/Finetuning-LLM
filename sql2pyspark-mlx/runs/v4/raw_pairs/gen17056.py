from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("balance") < 60000).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .filter(F.col("t.amount") <= 1300)
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("t.account_id"),
        F.col("t.amount"),
    )
    .orderBy(F.col("account_id").desc())
)
