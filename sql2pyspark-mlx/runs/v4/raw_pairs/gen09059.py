from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.to_date(F.col("t.txn_time")) >= "2024-02-01")
    .select(
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("t.account_id"),
    )
    .orderBy(F.col("account_id").desc())
)
