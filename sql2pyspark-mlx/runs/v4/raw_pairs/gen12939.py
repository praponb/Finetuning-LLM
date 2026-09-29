from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("txn_type") == "DEBIT").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("a.status"),
    )
    .orderBy(F.col("id").desc(), "opened_date")
)
