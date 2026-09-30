from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("account_id") == 40).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .select(F.col("a.id"), F.col("a.status"), F.col("a.account_type"))
    .orderBy("account_type", "id")
    .limit(5)
)
