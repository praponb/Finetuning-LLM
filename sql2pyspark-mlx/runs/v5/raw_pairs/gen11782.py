from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") >= F.to_timestamp(F.lit("2023-10-03 05:00:00"))))
    .withColumn("amount_whole", F.ceil("amount"))
    .withColumn("merchant_lower", F.lower("merchant"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("NONE")))
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy("merchant")
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("session_start_flag"))).alias("refund_total_session_start_flag"),
        F.max("txn_time").alias("last_txn_time"),
        F.min("txn_time").alias("first_seen"),
    )
    .orderBy(F.col("refund_total_session_start_flag").desc())
)
