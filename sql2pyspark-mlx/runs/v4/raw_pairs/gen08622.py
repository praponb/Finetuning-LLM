from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("a.customer_id"),
        F.col("a.opened_date"),
        F.col("t.txn_type"),
        F.lower(F.col("a.status")).alias("status_lower"),
    )
    .orderBy("txn_type")
)
