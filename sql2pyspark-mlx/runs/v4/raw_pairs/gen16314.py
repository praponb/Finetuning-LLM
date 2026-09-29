from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .orderBy(F.col("stddev_amount").desc())
)
