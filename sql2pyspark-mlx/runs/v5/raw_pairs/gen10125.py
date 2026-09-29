from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .select(
        F.col("t.is_flagged"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("c.email"),
        F.col("c.is_active"),
    )
    .orderBy(F.col("customer_id").desc())
)
