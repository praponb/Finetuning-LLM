from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.account_id").isin([1, 18, 23]) & F.col("a.customer_id").isNull())
    .select(
        F.col("t.id"),
        F.col("t.amount"),
        F.col("t.account_id"),
        F.col("t.merchant"),
        F.col("a.status"),
    )
)
