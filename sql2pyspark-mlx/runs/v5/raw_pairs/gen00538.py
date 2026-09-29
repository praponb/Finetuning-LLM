from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(F.sum("amount").alias("total_amount"))
    .orderBy("txn_type")
)
