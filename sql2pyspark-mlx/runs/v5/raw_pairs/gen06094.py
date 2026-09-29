from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(F.max("txn_time").alias("last_txn_time"))
)
