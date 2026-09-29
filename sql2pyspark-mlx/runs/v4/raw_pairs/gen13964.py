from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2023-12-05")
    .select("merchant", "txn_type")
    .distinct()
    .orderBy("merchant")
)
