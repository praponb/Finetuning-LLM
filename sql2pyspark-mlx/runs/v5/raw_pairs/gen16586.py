from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") < 3200) | (F.col("txn_type") != "CREDIT"))
    .select("id", "is_flagged", "amount", F.lower("txn_type").alias("txn_type_lower"))
)
