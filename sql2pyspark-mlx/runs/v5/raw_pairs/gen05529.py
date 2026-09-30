from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") | F.col("merchant").isNull())
    .select(
        "id",
        "amount",
        "is_flagged",
        "txn_type",
        F.concat_ws("-", "merchant", "txn_type").alias("merchant_txn_type_key"),
    )
)
