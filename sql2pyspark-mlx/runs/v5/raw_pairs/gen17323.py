from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 31)
    .groupBy("txn_type")
    .pivot("merchant", ["Amazon", "7-Eleven", "Lazada"])
    .agg(F.max("amount"))
    .orderBy("txn_type")
)
