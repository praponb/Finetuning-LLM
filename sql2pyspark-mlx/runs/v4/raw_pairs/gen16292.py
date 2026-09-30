from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .filter(F.col("t.txn_type") == "CREDIT")
    .select(
        F.col("t.account_id"),
        F.col("t.merchant"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("a.opened_date"),
    )
    .orderBy("merchant")
)
