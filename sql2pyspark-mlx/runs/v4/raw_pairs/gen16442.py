from pyspark.sql import functions as F

src = df_transactions.filter(F.col("is_flagged"))

transactions_stats = (
    src
    .groupBy("txn_type")
    .agg(
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = transactions_stats.filter(F.col("grab_count") >= 5)
