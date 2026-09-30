from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.customer_id") == 6)
    .select(F.col("a.opened_date"), F.col("a.account_type"), F.col("t.txn_type"))
    .orderBy(F.col("account_type").desc())
)
