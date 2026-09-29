from pyspark.sql import functions as F

result = (
    df_transactions
    .select("id", "txn_time", F.lower("txn_type").alias("txn_type_lower"))
    .orderBy("id")
    .limit(1)
)
