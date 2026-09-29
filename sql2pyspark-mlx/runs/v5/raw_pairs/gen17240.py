from pyspark.sql import functions as F

result = (
    df_transactions
    .select("account_id", "merchant", F.to_date("txn_time").alias("txn_time_date"))
)
