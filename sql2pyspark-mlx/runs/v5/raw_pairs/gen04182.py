from pyspark.sql import functions as F

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("account_id") == 6))
    .select(
        "*",
        F.trim("txn_type").alias("clean_txn_type"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_clean"),
    )
)

result = (
    filtered
    .groupBy("merchant", "account_id")
    .pivot("txn_type", ["DEBIT", "FEE"])
    .agg(F.sum("amount"))
)
