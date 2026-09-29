from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter(F.col("txn_type") != "FEE")
    .withColumn("txn_type_lower", F.lower("txn_type"))
)

transactions_stats = (
    filtered_transactions
    .groupBy("merchant")
    .agg(
        F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0)).alias("lazada_count"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
    )
)

result = transactions_stats
