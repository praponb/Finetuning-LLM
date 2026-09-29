from pyspark.sql import functions as F

result = (
    df_transactions
    .withColumn("quarter_start", F.date_trunc("quarter", "txn_time"))
    .rollup("quarter_start")
    .agg(
        F.max("txn_time").alias("last_seen"),
        F.min("txn_time").alias("started_at"),
        F.avg(F.when(F.col("merchant") == "Shell", F.col("amount"))).alias("shell_avg_amount"),
        F.grouping_id().alias("gid"),
    )
)
