from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "REFUND", "CREDIT"]) & F.col("account_id").isin([4, 11, 32, 39]))
    .orderBy(F.col("id").desc())
)
