from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("amount") <= 2000).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("a.account_type"),
    )
)
