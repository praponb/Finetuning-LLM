from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") < "2025-01-19") | (F.col("txn_type") == "REFUND"))
    .select("id", "txn_time")
    .orderBy(F.col("id").desc())
)
