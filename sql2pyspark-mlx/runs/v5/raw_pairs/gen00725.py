from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .select(
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("t.is_flagged"),
        F.col("t.txn_type"),
        F.col("t.merchant"),
        F.when(F.col("t.is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)
