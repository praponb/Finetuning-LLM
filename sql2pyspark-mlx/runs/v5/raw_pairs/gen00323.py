from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") < 1100)
    .select("id", "amount", "txn_time", "txn_type")
)
