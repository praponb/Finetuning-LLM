from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(
        F.count("*").alias("transaction_count"),
        F.max("txn_time").alias("latest_txn_time"),
    )
    .filter(F.col("transaction_count") > 6)
    .select("txn_type", "txn_month", "transaction_count", "latest_txn_time")
    .orderBy("txn_type", "latest_txn_time")
)
