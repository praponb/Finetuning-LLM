from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy("session_no", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy(F.col("session_id").desc(), "id")
w_grp = Window.partitionBy("merchant").orderBy("session_no", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("merchant").orderBy("session_id", "id")
w9 = Window.orderBy("last4_sum_session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy(F.col("last4_avg_session_id").asc_nulls_last())

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

gaps_2 = (
    sessionized
    .withColumn("is_new_session_2", F.when(F.col("txn_time") - F.lag("txn_time").over(w3) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_transactions = (
    gaps_2
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_new_session",
        "session_id",
        F.sum("is_new_session_2").over(w4).alias("session_no"),
    )
)

scored = (
    sessions_transactions
    .select(
        "id",
        "merchant",
        "is_flagged",
        "session_id",
        "session_no",
        F.sum("session_id").over(w5).alias("last4_sum_session_id"),
        F.avg("session_id").over(w5).alias("last4_avg_session_id"),
        F.count("session_id").over(w5).alias("last4_count_session_id"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

metrics = (
    scored
    .filter(F.col("session_no").between(6, 7))
    .withColumn("last3_sum_session_id", F.sum("session_id").over(w_grp))
    .withColumn("last3_count_session_id", F.count("session_id").over(w_grp))
    .withColumn("last3_min_session_id", F.min("session_id").over(w_grp))
    .withColumn("last3_max_session_id", F.max("session_id").over(w_grp))
)

metrics_transactions = (
    metrics
    .select(
        "id",
        "session_id",
        "last4_sum_session_id",
        "last4_avg_session_id",
        "last3_count_session_id",
        F.ntile(5).over(w8).alias("session_id_bucket"),
        F.ntile(3).over(w8).alias("session_id_bucket_2"),
    )
)

scored_transactions = (
    metrics_transactions
    .select(
        "*",
        F.last("last4_sum_session_id", ignorenulls=True).over(w9).alias("last_known_last4_sum_session_id"),
        F.sum("session_id_bucket").over(w10).alias("cumulative_session_id_bucket"),
        F.dense_rank().over(w11).alias("last4_avg_session_id_dense_rank"),
    )
)

result = (
    scored_transactions
    .groupBy("session_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("session_id", 0.9).alias("session_id_p90"),
        F.sum("session_id_bucket").alias("total_session_id_bucket"),
        F.count("*").alias("cnt"),
    )
)
