from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(~((F.col("a.opened_date") < "2025-06-01") & (F.col("a.opened_date") <= "2023-06-01")))
    .select(F.col("a.account_type"), F.col("t.txn_type"), F.col("t.account_id"))
)
