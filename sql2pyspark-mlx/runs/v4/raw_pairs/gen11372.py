from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 1)
    .groupBy(
        F.when(F.col("amount") >= 4000, "high").when(F.col("amount") >= 600, "mid").otherwise("low").alias("amount_bucket"),
        "txn_type",
    )
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)
