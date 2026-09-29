from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.balance"), F.col("b.account_type"))
)
