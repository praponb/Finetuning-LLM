from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["DEBIT", "CREDIT"]))
    .select("txn_type", "is_flagged")
    .orderBy("txn_type")
)
