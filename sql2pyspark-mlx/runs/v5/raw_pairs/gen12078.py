from pyspark.sql import functions as F

result = (
    df_transactions
    .rollup(F.year("txn_time").alias("txn_year"), "txn_type")
    .agg(F.min("txn_time").alias("first_txn_time"))
)
