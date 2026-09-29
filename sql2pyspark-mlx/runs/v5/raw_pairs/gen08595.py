from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("balance") >= 58000).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .filter(F.col("t.account_id").isin([17, 26, 37]))
    .select(
        F.col("t.id"),
        F.col("t.account_id"),
        F.col("t.amount"),
        F.col("t.is_flagged"),
    )
)
