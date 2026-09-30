from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "is_flagged",
        "amount",
        "account_id",
        F.concat_ws("/", "txn_type", "merchant").alias("txn_type_merchant_key"),
    )
    .orderBy(F.col("account_id").desc())
)
