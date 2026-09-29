from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "merchant",
        "account_id",
        "txn_type",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.concat("merchant", F.lit("/"), "txn_type").alias("merchant_txn_type"),
    )
)
