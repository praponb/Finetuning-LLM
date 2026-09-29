from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 1800)
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.max("txn_time").alias("latest_txn_time"),
    )
    .orderBy("stddev_amount")
)
