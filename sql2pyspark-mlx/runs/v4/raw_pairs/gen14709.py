from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "outer")
    .filter(F.col("a.status").isin(["FROZEN", "CLOSED", "OPEN"]))
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("a.opened_date"),
        F.col("c.segment"),
    )
)
