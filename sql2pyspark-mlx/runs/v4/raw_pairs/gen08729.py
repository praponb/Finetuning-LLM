from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy(F.col("amount").desc())
w4 = Window.partitionBy("txn_type").orderBy("prev_txn_time", "id")
w5 = Window.partitionBy("txn_type").orderBy("prev_txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("session_start_flag").desc(), "id")
w7 = Window.partitionBy("txn_type", "merchant").orderBy("session_start_flag", "id")
w8 = Window.partitionBy("account_id", "txn_type").orderBy("island_id", "id")
w9 = Window.partitionBy("account_id")
w_hist = Window.partitionBy("account_id").orderBy("amount_share", "id").rowsBetween(-2, -1)
w11 = Window.partitionBy("account_id", "merchant")
w12 = Window.partitionBy("txn_type").orderBy(F.col("session_no").desc(), "id")
w13 = Window.partitionBy("account_id").orderBy("session_start_flag_bucket", "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

deduped = (
    sessions_transactions
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

flagged = (
    deduped
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time") - F.lag("prev_txn_time").over(w4) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_burst").over(w5))

metrics = (
    islands
    .withColumn("session_start_flag_bucket", F.ntile(5).over(w6))
    .withColumn("seq_no", F.row_number().over(w7))
    .withColumn("island_id_bucket", F.ntile(10).over(w8))
    .withColumn("cumulative_amount", F.sum("amount").over(w5))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w9)), 4))
)

metrics_transactions = (
    metrics
    .select(
        "*",
        F.avg("island_id").over(w_hist).alias("island_id_avg_2_prior"),
        F.stddev_samp("amount").over(w11).alias("account_id_stddev_samp_amount"),
    )
)

filtered = (
    metrics_transactions
    .filter(F.col("island_id") == 4)
    .withColumn("is_new_burst_level", F.when(F.col("is_new_burst") >= 1, "High").when(F.col("is_new_burst") >= 0, "Medium").otherwise("Low"))
)

scored = (
    filtered
    .withColumn("session_no_bucket", F.ntile(5).over(w12))
    .withColumn("session_start_flag_bucket_bucket", F.ntile(4).over(w13))
)

result = (
    scored
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "island_id",
        "is_new_burst",
        "session_no",
        F.col("sum_transaction_amount"),
    )
    .orderBy("txn_type")
)
