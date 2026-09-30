from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.customer_id").isNotNull() | F.col("a.opened_date").between("2023-12-01", "2024-12-01"))
    .select(
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("t.txn_time"),
        F.col("t.txn_type"),
        F.concat(F.col("a.account_type"), F.lit(" "), F.col("a.status")).alias("account_type_status"),
    )
    .orderBy(F.col("txn_time").desc())
)
