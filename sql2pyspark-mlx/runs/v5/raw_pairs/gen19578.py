from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .pivot("txn_type", ["FEE", "DEBIT", "REFUND", "CREDIT"])
    .agg(F.count("amount"))
)
