from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "amount",
        "is_flagged",
        "txn_time",
        F.round("amount", 1).alias("amount_rounded"),
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
    )
)
