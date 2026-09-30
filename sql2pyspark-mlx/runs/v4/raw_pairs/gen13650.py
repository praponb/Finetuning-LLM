from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.min("txn_time").alias("min_txn_time"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") > 18000)
)
