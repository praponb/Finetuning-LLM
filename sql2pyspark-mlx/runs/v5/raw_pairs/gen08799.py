from pyspark.sql import functions as F

src = df_transactions.filter(F.col("merchant") == "7-Eleven")

transactions_stats = (
    src
    .groupBy("account_id")
    .agg(
        F.count("*").alias("row_count"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    transactions_stats
    .orderBy(F.col("account_id").desc())
    .limit(5)
)
