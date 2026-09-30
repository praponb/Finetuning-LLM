from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type").isin(["REFUND", "FEE", "CREDIT"]) & F.col("merchant").isin(["Amazon", "7-Eleven"])) | F.col("account_id").isin([10, 34]))
    .select("id", "merchant", "account_id")
)
