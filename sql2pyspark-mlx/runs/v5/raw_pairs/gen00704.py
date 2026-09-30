from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([2, 14]) & (F.col("is_flagged") | (F.to_date("txn_time") < "2023-04-21")))
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(F.max("txn_time").alias("last_txn_time"))
)
