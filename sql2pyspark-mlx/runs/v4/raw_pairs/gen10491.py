from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("txn_type").isin(["REFUND", "FEE"]))
    .select(
        "id",
        "account_id",
        "is_flagged",
        "txn_type",
        F.upper("merchant").alias("merchant_upper"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy(F.col("is_flagged_flag").desc())
)
