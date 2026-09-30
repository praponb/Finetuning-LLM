from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") == "REFUND") & (F.col("txn_time") >= "2025-08-01"))
    .select(
        "id",
        "account_id",
        "is_flagged",
        F.length("merchant").alias("merchant_length"),
    )
)
