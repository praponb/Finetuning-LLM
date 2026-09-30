from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .pivot("txn_type", ["REFUND", "CREDIT"])
    .agg(F.count("amount"))
    .orderBy("merchant")
)
