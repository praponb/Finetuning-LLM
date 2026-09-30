from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "inner")
    .select("src.*", F.col("a.account_type"), F.col("a.opened_date"), F.col("a.status"))
)
