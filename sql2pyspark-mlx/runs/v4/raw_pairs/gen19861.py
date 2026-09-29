from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") != "REFUND")
    .select(
        "id",
        "is_flagged",
        "account_id",
        F.substring("merchant", 1, 4).alias("merchant_prefix"),
    )
)
