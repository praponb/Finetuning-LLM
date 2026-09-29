from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id") == 12) & (F.col("account_id") == 10))
    .select("txn_time", F.date_trunc("MONTH", "txn_time").alias("txn_time_period"))
)
