from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "amount",
        F.concat_ws(",", "txn_type", "merchant").alias("txn_type_merchant_key"),
    )
)
