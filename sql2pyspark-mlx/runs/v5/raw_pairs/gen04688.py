from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .select(
        F.col("a.account_type"),
        F.col("t.txn_time"),
        F.col("t.merchant"),
        F.weekofyear(F.col("a.opened_date")).alias("opened_date_weekofyear"),
    )
    .orderBy("txn_time")
)
