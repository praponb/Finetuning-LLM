from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") != "Amazon")
    .groupBy(
        F.when(F.col("amount") >= 4400, "high").when(F.col("amount") >= 600, "mid").otherwise("low").alias("amount_bucket"),
        "txn_type",
    )
    .agg(F.min("txn_time").alias("first_txn_time"))
)
