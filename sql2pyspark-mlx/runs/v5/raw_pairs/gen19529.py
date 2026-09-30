from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .select(F.col("t.amount"), F.col("t.merchant"), F.col("a.opened_date"))
    .orderBy("amount")
)
