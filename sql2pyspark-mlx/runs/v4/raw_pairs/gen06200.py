from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([15, 30]))
    .select(
        "id",
        "txn_type",
        F.trim("txn_type").alias("clean_txn_type"),
        F.initcap(F.trim("merchant")).alias("merchant_title"),
    )
    .orderBy("txn_type", "merchant_title")
)
