from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.account_id",
        "src.amount",
        "src.txn_time",
        "src.merchant",
        F.col("d.status"),
    )
)
