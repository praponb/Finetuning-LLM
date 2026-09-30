from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "CREDIT")
    .groupBy("txn_type")
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") >= 4)
)
