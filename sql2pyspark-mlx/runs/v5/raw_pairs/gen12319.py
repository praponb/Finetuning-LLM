from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Starbucks", "Shell"]))
    .groupBy("merchant")
    .pivot("txn_type", ["REFUND", "FEE", "CREDIT", "DEBIT"])
    .agg(F.max("amount"))
)
