from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4300, "high").when(F.col("amount") >= 1200, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
    )
    .filter(F.col("avg_amount") >= 70)
)
