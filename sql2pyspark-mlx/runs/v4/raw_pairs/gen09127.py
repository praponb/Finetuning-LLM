from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Amazon", "Starbucks"]) & F.col("merchant").isin(["Lazada", "Grab", "Shell"]) & (F.col("txn_time") < "2023-08-02"))
)

agg = (
    valid_transactions
    .groupBy("txn_type")
    .agg(F.max("txn_time").alias("latest_txn_time"))
)

result = agg.orderBy("txn_type")
