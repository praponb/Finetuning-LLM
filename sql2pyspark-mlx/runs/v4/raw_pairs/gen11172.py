from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.customer_id").isNull())
    .select(
        F.col("a.opened_date"),
        F.col("t.txn_time"),
        F.col("t.is_flagged"),
        F.col("t.amount"),
    )
    .orderBy(F.col("amount").desc())
)
