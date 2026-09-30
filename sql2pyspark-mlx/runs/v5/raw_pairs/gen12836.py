from pyspark.sql import functions as F

filtered_transactions = df_transactions.filter(F.col("txn_type") == "FEE")

summary = (
    filtered_transactions
    .groupBy("is_flagged", "txn_type")
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.count("amount").alias("amount_count"),
    )
)

result = summary.filter(F.col("amount_count") >= 9)
