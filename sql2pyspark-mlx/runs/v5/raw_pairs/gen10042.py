from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .pivot("txn_type", ["REFUND", "DEBIT", "CREDIT", "FEE"])
    .agg(F.count("amount"))
)
