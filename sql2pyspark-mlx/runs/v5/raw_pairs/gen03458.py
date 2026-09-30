from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("t.amount"),
        F.col("t.account_id"),
        F.substring(F.col("t.merchant"), 1, 2).alias("merchant_prefix"),
    )
    .orderBy(F.col("merchant_prefix").desc())
)
