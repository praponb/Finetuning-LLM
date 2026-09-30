from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .pivot("txn_type", ["CREDIT", "FEE"])
    .agg(F.max("amount"))
)
