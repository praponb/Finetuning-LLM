from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .pivot("txn_type", ["REFUND", "FEE"])
    .agg(F.sum("amount"))
    .orderBy("merchant")
)
