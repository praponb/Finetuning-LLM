from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(F.max("txn_time").alias("last_txn_time"))
    .orderBy("last_txn_time")
)
