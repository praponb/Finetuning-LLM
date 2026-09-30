from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .select(
        F.col("t.txn_type"),
        F.col("t.amount"),
        F.col("t.is_flagged"),
        F.col("a.customer_id"),
        F.col("a.opened_date"),
        F.col("a.balance"),
    )
    .orderBy("txn_type")
)
