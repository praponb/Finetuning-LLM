from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.hour("txn_time").between(7, 15)).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.account_type") == "SAVINGS")
    .select(F.col("a.id"), F.col("a.account_type"), F.col("a.opened_date"))
)
