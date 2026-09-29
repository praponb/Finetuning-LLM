from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(F.col("account_id").isin([8, 38]))
    .withColumn("txn_type_merchant", F.concat("txn_type", F.lit(" "), "merchant"))
)

transactions_stats = (
    recent_transactions
    .groupBy(
        F.when(F.col("amount") >= 3600, "high").when(F.col("amount") >= 900, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    transactions_stats
    .filter(F.col("max_amount") >= 3300)
    .select("amount_bucket", "min_txn_time", "max_amount")
    .orderBy("amount_bucket")
    .limit(10)
)
