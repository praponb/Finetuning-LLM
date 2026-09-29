from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .select(F.col("a.id"), F.col("a.status"), F.col("a.account_type"))
    .orderBy("status", "id")
    .limit(3)
)
