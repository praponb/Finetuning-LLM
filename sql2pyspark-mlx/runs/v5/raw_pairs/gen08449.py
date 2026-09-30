from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~((F.col("txn_type") == "CREDIT") & (F.col("amount") < 3100)))
    .select("id", "amount", "merchant", "txn_time")
)
