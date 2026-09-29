from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant", "is_flagged")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("txn_time").alias("latest_txn_time"),
    )
    .filter(F.col("avg_amount") > 23)
    .orderBy("merchant", "is_flagged")
    .limit(10)
)
