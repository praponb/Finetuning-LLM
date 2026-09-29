from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .pivot("txn_type", ["DEBIT", "FEE", "REFUND", "CREDIT"])
    .agg(F.sum("amount"))
    .orderBy("merchant")
)
