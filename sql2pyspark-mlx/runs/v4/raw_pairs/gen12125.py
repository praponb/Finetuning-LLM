from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([22, 31]) | F.col("merchant").isin(["Grab", "Starbucks", "Lazada"]))
    .groupBy("merchant")
    .agg(F.min("txn_time").alias("min_txn_time"))
    .orderBy("min_txn_time", "merchant")
    .limit(5)
)
