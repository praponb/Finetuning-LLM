from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", "merchant")
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") > 2600)
    .select("txn_type", "merchant", "min_amount")
)
