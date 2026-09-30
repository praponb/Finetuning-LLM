from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant", "txn_type")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") >= 8)
    .select("merchant", "txn_type", "avg_amount", "amount_count")
    .orderBy(F.col("avg_amount").desc(), "amount_count")
)
