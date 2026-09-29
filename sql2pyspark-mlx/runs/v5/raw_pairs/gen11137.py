from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 2800)
    .groupBy("merchant", "txn_type")
    .agg(F.sum("amount").alias("total_amount"))
)
