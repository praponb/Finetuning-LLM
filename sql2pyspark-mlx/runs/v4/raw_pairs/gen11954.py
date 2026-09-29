from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 27)
    .select(
        "id",
        "is_flagged",
        "txn_time",
        "amount",
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_filled"),
    )
)
