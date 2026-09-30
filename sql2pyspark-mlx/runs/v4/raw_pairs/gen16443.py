from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2024-06-01")
    .select(
        "id",
        "is_flagged",
        "account_id",
        F.upper("merchant").alias("merchant_upper"),
    )
    .orderBy(F.col("id").desc(), "merchant_upper")
    .limit(1)
)
