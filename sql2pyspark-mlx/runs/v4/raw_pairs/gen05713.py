from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isNull() & (F.col("txn_time") < "2025-07-22") & (F.col("account_id") == 36))
    .select(
        "merchant",
        "account_id",
        "is_flagged",
        F.trim("merchant").alias("clean_merchant"),
    )
    .orderBy("merchant")
)
