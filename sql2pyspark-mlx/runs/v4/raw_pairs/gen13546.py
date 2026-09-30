from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(F.max("amount").alias("max_amount"))
    .filter(F.col("max_amount") >= 2900)
    .select("txn_type", "max_amount")
)
