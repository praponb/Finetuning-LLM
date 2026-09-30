from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("a.account_type") == "CHECKING")
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("t.amount"),
        F.col("t.is_flagged"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
    )
)
