from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.customer_id").isNotNull())
    .select(
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("t.amount"),
        F.col("t.txn_time"),
    )
    .orderBy(F.col("status").desc(), "opened_date")
)
