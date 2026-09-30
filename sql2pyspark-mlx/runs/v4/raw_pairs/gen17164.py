from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(~F.col("t.is_flagged"))
    .groupBy(F.col("a.account_type"), F.col("a.status"))
    .agg(F.min(F.col("t.amount")).alias("min_amount"))
    .filter(F.col("min_amount") > 1100)
    .orderBy("min_amount")
)
