from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id") == 28) & ((F.col("txn_time") >= "2023-06-24") | F.hour("txn_time").between(7, 10)))
    .groupBy("txn_type")
    .agg(F.min("amount").alias("min_amount"))
)
