from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .pivot("merchant", ["Amazon", "Starbucks", "Shell"])
    .agg(F.avg("amount"))
)
