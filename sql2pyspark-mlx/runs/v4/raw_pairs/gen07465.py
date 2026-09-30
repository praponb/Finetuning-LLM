from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("txn_type") == "FEE") & F.col("account_id").isin([1, 8, 13, 38]))
)

transactions_stats = (
    filtered_transactions
    .groupBy("account_id", "is_flagged")
    .agg(F.max("amount").alias("max_amount"))
)

result = transactions_stats.select("account_id", "max_amount")
