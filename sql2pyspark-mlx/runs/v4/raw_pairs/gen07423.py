from pyspark.sql import functions as F

filtered_transactions = df_transactions.filter(~(F.col("is_flagged") & ~F.col("is_flagged")))

transactions_stats = (
    filtered_transactions
    .groupBy("account_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    transactions_stats
    .filter(F.col("amount_range") >= 59)
    .select("account_id", "amount_range")
    .orderBy("amount_range", "account_id")
    .limit(20)
)
