from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") != "CREDIT")
    .select("merchant", F.lower("merchant").alias("merchant_lower"))
    .orderBy("merchant_lower")
)
