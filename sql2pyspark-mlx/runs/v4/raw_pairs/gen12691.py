from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .pivot("txn_type", ["FEE", "REFUND", "DEBIT", "CREDIT"])
    .agg(F.count("amount"))
    .orderBy("is_flagged")
)
