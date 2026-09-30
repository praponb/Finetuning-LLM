from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.month(F.col("a.opened_date")).isin([1, 3, 6]))
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("t.amount"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("c.credit_limit"),
        F.col("c.country"),
    )
)
