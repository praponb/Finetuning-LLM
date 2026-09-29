from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([17, 28, 32]))
    .select(
        "id",
        "txn_type",
        F.lower("merchant").alias("merchant_lower"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy("merchant_lower")
)
