from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([13, 23]))
    .select(
        "id",
        "amount",
        "txn_type",
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_filled"),
    )
    .orderBy("merchant_filled")
)
