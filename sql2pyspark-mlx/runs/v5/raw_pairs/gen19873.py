from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .groupBy(F.col("a.status"))
    .agg(F.min(F.col("t.amount")).alias("min_amount"))
    .orderBy("status")
)
