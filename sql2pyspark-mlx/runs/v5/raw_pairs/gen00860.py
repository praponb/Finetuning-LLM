from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(F.col("a.account_type"), F.col("t.merchant"))
    .orderBy(F.col("account_type").desc())
)
