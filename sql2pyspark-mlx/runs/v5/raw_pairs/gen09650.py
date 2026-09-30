from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg(F.max("amount").alias("max_amount"))
    .filter(F.col("max_amount") > 3800)
    .orderBy("max_amount")
)
