from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 26)
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)
