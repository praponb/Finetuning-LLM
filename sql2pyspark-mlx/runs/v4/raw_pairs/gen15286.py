from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2024-03-01")
    .select("id", "txn_time")
    .orderBy("txn_time", "id")
    .limit(5)
)
