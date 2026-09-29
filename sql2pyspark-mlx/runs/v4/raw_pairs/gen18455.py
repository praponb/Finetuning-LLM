from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "account_id",
        "is_flagged",
        "txn_type",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)
