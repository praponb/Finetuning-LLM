from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc_nulls_first(), "id")
w2 = Window.partitionBy("account_id", "merchant").orderBy(F.col("amount").desc())
w3 = Window.partitionBy("merchant")
w_running = Window.partitionBy("account_id").orderBy("amount", "id")
w5 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w7 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("merchant").orderBy("prev_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("prev_amount").desc(), "id")
w10 = Window.orderBy(F.col("prev_amount").asc_nulls_last())
w_ordered = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy("prev_amount_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy("running_count_prev_amount_rank", "id")
w14 = Window.orderBy("running_min_prev_amount_rank", "id")

latest = (
    df_transactions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    latest
    .select(
        "id",
        "txn_time",
        "merchant",
        "dedup_rank",
        F.cume_dist().over(w2).alias("amount_cume_dist"),
        F.avg("amount").over(w3).alias("merchant_avg_amount"),
        F.lag("amount", 1).over(w_running).alias("prev_amount"),
        F.sum("amount").over(w5).alias("running_amount"),
    )
)

with_prev = with_windows.withColumn("prev_txn_time", F.lag("txn_time").over(w6))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w7))

with_windows_transactions = (
    sessionized
    .filter(F.col("merchant").isin(["Shell", "Starbucks"]))
    .select(
        "id",
        "txn_time",
        "prev_amount",
        "running_amount",
        "prev_txn_time",
        "session_start_flag",
        F.stddev_samp("session_no").over(w3).alias("merchant_stddev_samp_session_no"),
        F.sum("session_start_flag").over(w8).alias("running_session_start_flag"),
    )
)

metrics = (
    with_windows_transactions
    .select(
        "*",
        F.ntile(4).over(w9).alias("prev_amount_bucket"),
        F.rank().over(w10).alias("prev_amount_rank"),
    )
)

enriched = (
    metrics
    .withColumn("running_sum_prev_amount_rank", F.sum("prev_amount_rank").over(w_ordered))
    .withColumn("running_count_prev_amount_rank", F.count("prev_amount_rank").over(w_ordered))
    .withColumn("running_min_prev_amount_rank", F.min("prev_amount_rank").over(w_ordered))
    .withColumn("last_known_prev_amount", F.last("prev_amount", ignorenulls=True).over(w12))
)

result = (
    enriched
    .select(
        "id",
        "txn_time",
        "running_amount",
        "prev_txn_time",
        "merchant_stddev_samp_session_no",
        "running_session_start_flag",
        "prev_amount_bucket",
        "running_sum_prev_amount_rank",
        F.lead("running_amount").over(w13).alias("next_running_amount"),
        F.lag("running_amount").over(w13).alias("prev_running_amount"),
        F.lead("last_known_prev_amount").over(w13).alias("next_last_known_prev_amount"),
        F.lag("last_known_prev_amount").over(w13).alias("prev_last_known_prev_amount"),
        F.row_number().over(w13).alias("seq_no"),
        F.lag("last_known_prev_amount", 1).over(w14).alias("prev_last_known_prev_amount_2"),
    )
)
