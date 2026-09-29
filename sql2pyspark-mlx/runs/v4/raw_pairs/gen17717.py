from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").isNull() & (F.col("amount") <= 3100))
    .select("id", "txn_time")
)
