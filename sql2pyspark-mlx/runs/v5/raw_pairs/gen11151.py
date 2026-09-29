from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.opened_date").between("2023-03-01", "2023-05-05"))
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("t.txn_type"),
        F.col("a.balance"),
        F.col("a.account_type"),
        F.col("a.status"),
    )
    .orderBy("txn_time")
)
