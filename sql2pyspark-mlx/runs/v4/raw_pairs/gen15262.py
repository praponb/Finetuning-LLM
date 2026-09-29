from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(6, 14))
    .groupBy(
        F.when(F.col("amount") >= 3900, "high").when(F.col("amount") >= 1200, "mid").otherwise("low").alias("amount_bucket"),
        F.date_format("txn_time", "yyyy-MM").alias("txn_month"),
    )
    .agg(F.min("txn_time").alias("min_txn_time"))
    .orderBy("min_txn_time", "amount_bucket", "txn_month")
    .limit(10)
)
