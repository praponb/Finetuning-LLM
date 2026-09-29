from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(F.col("is_flagged"))
    .withColumn("txn_time_period", F.date_trunc("week", "txn_time"))
)

agg = (
    recent_transactions
    .groupBy("merchant")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    agg
    .select("merchant", "avg_amount")
    .orderBy("merchant")
    .limit(1)
)
