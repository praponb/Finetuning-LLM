from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter((F.col("account_id") == 25) & F.col("merchant").isin(["7-Eleven", "Grab"]) & F.col("account_id").isin([23, 33, 38, 40]))
)

summary = (
    filtered_transactions
    .groupBy("account_id")
    .agg(F.count("*").alias("transaction_count"))
)

result = summary.filter(F.col("transaction_count") > 4)
