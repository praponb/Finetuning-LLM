from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.max("amount").alias("max_amount"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy(F.col("max_amount").desc())
)
