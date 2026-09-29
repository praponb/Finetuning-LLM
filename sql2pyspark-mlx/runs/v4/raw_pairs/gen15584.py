from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id", F.hour("txn_time").alias("txn_hour"), "txn_type")
    .agg(F.max("amount").alias("max_amount"))
    .orderBy(F.col("account_id").desc())
)
