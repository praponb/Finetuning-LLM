from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("a.account_type") != "CREDIT")
    .select(
        F.col("t.is_flagged"),
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("t.is_flagged").cast("int").alias("is_flagged_flag"),
    )
)
