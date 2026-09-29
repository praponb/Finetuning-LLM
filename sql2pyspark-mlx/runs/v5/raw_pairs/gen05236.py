from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "left")
    .select(
        "e.id",
        "e.account_id",
        "e.txn_type",
        "e.is_flagged",
        F.col("a.status"),
        F.col("a.balance"),
        F.col("a.account_type"),
    )
)
