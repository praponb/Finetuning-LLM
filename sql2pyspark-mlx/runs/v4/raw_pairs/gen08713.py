from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "REFUND")
    .groupBy(
        F.when(F.col("amount") >= 3200, "high").when(F.col("amount") >= 2000, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)
