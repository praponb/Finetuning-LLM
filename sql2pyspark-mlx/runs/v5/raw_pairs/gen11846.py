from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(((F.col("txn_type") == "FEE") & F.col("is_flagged")) | F.col("txn_type").isin(["DEBIT", "REFUND", "FEE"]))
    .orderBy(F.col("amount").desc(), "merchant")
)
