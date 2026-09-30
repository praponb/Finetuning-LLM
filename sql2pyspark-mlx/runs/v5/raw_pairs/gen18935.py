from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("opened_date").between("2023-04-01", "2024-08-01")).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .select(F.col("t.id"), F.col("t.txn_type"), F.col("t.account_id"))
    .orderBy("txn_type", "id")
    .limit(5)
)
