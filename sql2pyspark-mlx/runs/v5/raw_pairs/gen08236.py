from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Amazon", "Starbucks", "Shell"])
    .agg(F.max("amount"))
)
