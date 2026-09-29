from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("account_id") == 26))
    .select(
        "*",
        F.length("merchant").alias("merchant_length"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_filled"),
    )
)

result = (
    valid
    .groupBy("account_id")
    .pivot("txn_type", ["REFUND", "DEBIT"])
    .agg(F.sum("amount"))
)
