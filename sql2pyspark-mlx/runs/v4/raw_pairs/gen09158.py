from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.hour(F.col("t.txn_time")).between(10, 16) | F.col("a.customer_id").isNull())
    .select(
        F.col("t.id"),
        F.col("t.account_id"),
        F.col("t.txn_type"),
        F.col("a.status"),
        F.upper(F.col("t.txn_type")).alias("txn_type_upper"),
    )
    .orderBy("txn_type_upper")
)
