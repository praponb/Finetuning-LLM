from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("status").isin(["FROZEN", "OPEN"])).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .filter(F.col("t.account_id").isin([6, 23]))
    .select(F.col("t.id"), F.col("t.amount"))
)
