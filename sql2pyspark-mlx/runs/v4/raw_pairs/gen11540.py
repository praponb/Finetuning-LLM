from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0)).alias("starbucks_count"),
    )
    .filter(F.col("starbucks_count") < 54)
    .select("is_flagged", "last_txn_time", "starbucks_count")
)
