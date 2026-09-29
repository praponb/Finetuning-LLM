from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.count("*").alias("row_count"),
        F.min("amount").alias("min_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .filter(F.col("stddev_amount") > 53)
    .orderBy("txn_type", F.col("stddev_amount").desc())
)
