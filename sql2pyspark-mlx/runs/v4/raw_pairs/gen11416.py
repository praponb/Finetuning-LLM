from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.opened_date") < "2024-08-15")
    .select(
        F.col("t.is_flagged"),
        F.col("t.amount"),
        F.col("t.account_id"),
        F.col("a.account_type"),
        F.col("a.balance"),
    )
)
