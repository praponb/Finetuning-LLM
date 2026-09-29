from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "is_flagged",
        "txn_type",
        "merchant",
        F.month("txn_time").alias("txn_time_month"),
    )
)
