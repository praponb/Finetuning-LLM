from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") != "REFUND")
    .select("merchant", "txn_type")
    .distinct()
    .orderBy("merchant", "txn_type")
    .limit(5)
)
