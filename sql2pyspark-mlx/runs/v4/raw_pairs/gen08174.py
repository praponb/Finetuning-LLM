from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant", F.year("txn_time").alias("txn_year"))
    .agg(F.count("*").alias("num_transactions"))
    .orderBy("num_transactions", "merchant", "txn_year")
    .limit(50)
)
