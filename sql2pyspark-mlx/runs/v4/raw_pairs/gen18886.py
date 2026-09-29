from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3800, "high").when(F.col("amount") >= 1500, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("amount")).otherwise(0)).alias("starbucks_amount"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
    )
    .orderBy("last_txn_time", "amount_bucket")
    .limit(5)
)
