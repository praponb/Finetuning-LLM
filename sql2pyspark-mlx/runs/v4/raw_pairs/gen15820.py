from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") == "REFUND") & (F.col("txn_type") != "DEBIT") & F.col("txn_type").isin(["FEE", "DEBIT", "REFUND"]))
    .select("id", "is_flagged", "merchant", "txn_time")
    .orderBy("txn_time")
)
