from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("t.account_id").isin([22, 24, 40]))
    .select(
        F.col("t.amount"),
        F.col("t.txn_time"),
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("c.signup_date"),
        F.col("c.is_active"),
    )
    .orderBy(F.col("amount").desc())
)
