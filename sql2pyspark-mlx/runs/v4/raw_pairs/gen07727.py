from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("account_id").isin([6, 28]))
    .withColumn("is_7_eleven", F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0))
)

summary = (
    base
    .groupBy("txn_type")
    .agg(F.min("txn_time").alias("min_txn_time"))
)

result = (
    summary
    .select("txn_type", "min_txn_time")
    .orderBy(F.col("txn_type").desc())
)
