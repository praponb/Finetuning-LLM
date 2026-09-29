from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter((F.col("a.balance") >= 72000) & (F.col("t.txn_type") != "CREDIT"))
    .select(
        F.col("t.is_flagged"),
        F.col("a.customer_id"),
        F.col("a.balance"),
        F.col("c.city"),
        F.col("c.segment"),
    )
)
