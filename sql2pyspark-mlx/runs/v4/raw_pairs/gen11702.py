from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.filter(F.col("opened_date") <= "2024-07-12").alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left_semi")
    .select(F.col("transactions.id"), F.col("transactions.amount"))
    .orderBy("id")
)
