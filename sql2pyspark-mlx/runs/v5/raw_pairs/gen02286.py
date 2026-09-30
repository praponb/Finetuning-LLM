from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4100, "high").when(F.col("amount") >= 1700, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count("amount").alias("amount_count"),
        F.max("txn_time").alias("latest_txn_time"),
    )
    .orderBy("amount_bucket")
    .limit(5)
)
