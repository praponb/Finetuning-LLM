from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Amazon", "Starbucks"])
    .agg(F.max("amount"))
    .orderBy("txn_type")
)
