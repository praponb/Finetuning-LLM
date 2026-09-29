from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.max("amount").alias("max_amount"),
    )
)
