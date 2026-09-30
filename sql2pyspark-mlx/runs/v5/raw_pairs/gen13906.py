from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") == "REFUND") & (F.col("is_flagged") | (F.col("txn_type") == "REFUND")))
    .groupBy("is_flagged", "txn_type")
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.count("*").alias("total_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy("latest_txn_time")
)
