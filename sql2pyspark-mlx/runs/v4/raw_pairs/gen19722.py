from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") < "2023-09-05")
    .groupBy("merchant")
    .pivot("txn_type", ["DEBIT", "REFUND", "FEE"])
    .agg(F.avg("amount"))
    .orderBy("merchant")
)
