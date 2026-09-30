from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    islands
    .cube(
        F.when(F.col("session_start_flag") >= 1, "HIGH").otherwise("LOW").alias("session_start_flag_tier"),
        F.when(F.col("amount") >= 3100, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
        F.percentile_approx("amount", 0.95).alias("amount_p95_2"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.max(F.when(F.col("txn_type") == "DEBIT", F.col("session_start_flag"))).alias("debit_max_session_start_flag"),
    )
    .orderBy(F.col("avg_session_start_flag").desc())
)
