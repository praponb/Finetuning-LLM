from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.customer_id").isNotNull())
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("a.balance"),
    )
)
