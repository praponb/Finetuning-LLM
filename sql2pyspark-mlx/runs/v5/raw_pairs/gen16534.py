from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-07-15 14:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .withColumn("amount_rounded", F.round("amount", 1))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
)

result = (
    valid
    .groupBy("txn_type", "account_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.when(F.col("merchant") == "Starbucks", F.col("amount"))).alias("starbucks_avg_amount"),
        F.max("txn_time").alias("last_seen"),
    )
    .filter(F.col("num_rows") < 1000)
    .orderBy(F.col("num_rows").desc())
)
