from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("t.merchant"),
        F.col("t.is_flagged"),
        F.col("t.txn_time"),
        F.when(F.col("a.account_type").isin(["CREDIT", "CHECKING"]), "Group A").otherwise("Other").alias("account_type_group"),
    )
)
