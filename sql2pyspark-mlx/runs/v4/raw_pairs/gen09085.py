from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("amount") > 1700) & (F.col("txn_type") == "FEE"))
    .select("is_flagged", "amount", F.length("txn_type").alias("txn_type_length"))
)
