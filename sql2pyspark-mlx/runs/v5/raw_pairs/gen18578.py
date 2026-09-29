from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("merchant", "txn_type").orderBy("is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("running_avg_is_new_session").desc(), "id")
w5 = Window.partitionBy("txn_type")
w_grp = Window.orderBy("amount", "id")

with_gaps = (
    df_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

filtered = (
    sessions_transactions
    .filter(F.col("txn_time") > F.to_timestamp(F.lit("2023-08-08 17:00:00")))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_new_session",
        "session_id",
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
        F.lower("txn_type").alias("txn_type_lower"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "txn_type",
        "amount",
        "is_flagged",
        "session_id",
        "days_since_txn_time",
        F.max("is_new_session").over(w_running).alias("running_max_is_new_session"),
        F.avg("is_new_session").over(w_running).alias("running_avg_is_new_session"),
        F.sum("is_new_session").over(w_running).alias("running_sum_is_new_session"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "amount",
        "is_flagged",
        "session_id",
        "running_max_is_new_session",
        F.ntile(3).over(w_hist).alias("running_avg_is_new_session_bucket"),
        F.min("days_since_txn_time").over(w5).alias("txn_type_min_days_since_txn_time"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("running_avg_is_new_session_bucket") >= 7, "HIGH").otherwise("LOW").alias("running_avg_is_new_session_bucket_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
    .orderBy(F.col("running_avg_is_new_session_bucket_tier").desc_nulls_last())
)
