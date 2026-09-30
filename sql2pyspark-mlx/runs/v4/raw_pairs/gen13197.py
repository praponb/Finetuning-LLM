from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") < "2024-04-01")
    .select("id", "account_id", F.trim("txn_type").alias("clean_txn_type"))
)
