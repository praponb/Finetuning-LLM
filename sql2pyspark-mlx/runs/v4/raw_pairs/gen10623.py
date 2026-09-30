from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "FEE")
    .groupBy("account_id")
    .pivot("txn_type", ["REFUND", "DEBIT", "FEE", "CREDIT"])
    .agg(F.max("amount"))
)
