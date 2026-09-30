from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", "account_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") < 52)
)
