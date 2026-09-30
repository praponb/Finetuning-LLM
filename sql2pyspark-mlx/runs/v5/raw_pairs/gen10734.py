from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2025-06-21")
    .groupBy(
        F.when(F.col("amount") >= 4300, "high").when(F.col("amount") >= 1200, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") > 43)
)
