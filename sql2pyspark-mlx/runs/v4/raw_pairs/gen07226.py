from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([6, 10, 24, 38]))
    .groupBy("txn_type")
    .pivot("merchant", ["Lazada", "Amazon", "7-Eleven"])
    .agg(F.max("amount"))
    .orderBy("txn_type")
)
