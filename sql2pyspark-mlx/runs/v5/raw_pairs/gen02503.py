from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("customer_id") == 13).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .filter(~F.col("t.is_flagged"))
    .select(F.col("t.id"), F.col("t.merchant"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
