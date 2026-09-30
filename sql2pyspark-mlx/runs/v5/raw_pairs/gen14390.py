from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .orderBy(F.col("stddev_amount").desc(), F.col("avg_amount").desc())
)
