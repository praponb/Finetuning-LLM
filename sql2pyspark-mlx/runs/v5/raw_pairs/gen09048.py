from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 2000)
    .select(
        "amount",
        "is_flagged",
        F.concat_ws("/", "merchant", "txn_type").alias("merchant_txn_type_key"),
    )
    .orderBy("amount")
)
