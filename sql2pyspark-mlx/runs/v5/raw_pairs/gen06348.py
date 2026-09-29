from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2024-01-01")
    .select("id", "is_flagged", "txn_time", "account_id")
)
