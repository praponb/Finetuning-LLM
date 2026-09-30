from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .pivot("txn_type", ["CREDIT", "DEBIT", "REFUND", "FEE"])
    .agg(F.avg("amount"))
    .orderBy("merchant")
)
