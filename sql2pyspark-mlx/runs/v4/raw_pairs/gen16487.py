from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 28)
    .groupBy(
        "txn_type",
        F.date_format("txn_time", "yyyy-MM").alias("txn_month"),
        "merchant",
    )
    .agg(F.max("txn_time").alias("latest_txn_time"))
)
