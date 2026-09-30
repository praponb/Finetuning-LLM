from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "amount",
        "account_id",
        "is_flagged",
        F.trim("txn_type").alias("clean_txn_type"),
    )
)
