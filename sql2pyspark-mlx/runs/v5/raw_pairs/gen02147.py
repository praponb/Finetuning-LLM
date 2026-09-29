from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "merchant",
        "is_flagged",
        "account_id",
        F.date_trunc("week", "txn_time").alias("txn_time_period"),
    )
)
