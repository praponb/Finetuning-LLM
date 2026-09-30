from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_debit"),
        F.min("txn_time").alias("min_txn_time"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") > 2300)
    .drop("avg_amount")
)
