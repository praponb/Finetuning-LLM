from pyspark.sql import functions as F

valid_transactions = df_transactions.filter(F.col("is_flagged"))

summary = (
    valid_transactions
    .groupBy("is_flagged", "merchant")
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    summary
    .filter(F.col("avg_amount") < 3700)
    .orderBy("latest_txn_time")
)
