from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2024-02-25")
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .agg(F.count("amount").alias("amount_count"))
    .orderBy("txn_day")
    .limit(5)
)
