from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "amount",
        "merchant",
        F.concat_ws("-", "merchant", "txn_type").alias("merchant_txn_type_key"),
    )
    .orderBy("merchant")
)
