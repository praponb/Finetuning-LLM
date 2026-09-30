from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "right")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .select(
        F.col("t.id"),
        F.col("t.amount"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("c.email"),
    )
)
