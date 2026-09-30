from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Lazada")
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") > 61)
)
