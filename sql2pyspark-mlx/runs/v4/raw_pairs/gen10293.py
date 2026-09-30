from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.merchant") == "7-Eleven")
    .select(
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("t.is_flagged"),
        F.col("t.account_id"),
        F.col("t.merchant"),
    )
)
