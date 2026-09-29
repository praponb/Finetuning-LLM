from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter(F.col("amount").between(1200, 1600) & F.col("account_id").isin([17, 31, 36, 40]) & (F.col("txn_type") == "DEBIT"))
)

transactions_stats = (
    filtered_transactions
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_debit"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = transactions_stats
