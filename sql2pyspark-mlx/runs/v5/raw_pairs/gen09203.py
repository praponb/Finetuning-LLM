from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc_nulls_first(), "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("prev_txn_time", "id")
w5 = Window.partitionBy("merchant").orderBy("prev_txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("amount") >= 1300)
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.upper("merchant").alias("merchant_upper"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_clean"),
    )
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "txn_time", "merchant", "is_flagged", "merchant_clean")
)

with_prev = deduped.withColumn("prev_txn_time", F.lag("txn_time").over(w2))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w3))

prev_events = sessionized.withColumn("prev_prev_txn_time", F.lag("prev_txn_time").over(w4))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_prev_txn_time").isNull() | (F.col("prev_txn_time") > F.col("prev_prev_txn_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized_transactions = with_gaps.withColumn("burst_id_2", F.sum("is_new_burst").over(w5))

flagged_2 = (
    sessionized_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_transactions = (
    flagged_2
    .select(
        "id",
        "txn_time",
        "merchant",
        "prev_txn_time",
        "prev_prev_txn_time",
        "is_new_burst",
        "burst_id_2",
        F.sum("session_start_flag").over(w3).alias("session_id"),
    )
)

result = (
    sessions_transactions
    .cube(
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        F.when(F.col("session_id") >= 7, "HIGH").otherwise("LOW").alias("session_id_tier"),
    )
    .agg(
        F.min("prev_txn_time").alias("first_seen"),
        F.sum(F.when(F.col("merchant") == "Shell", F.col("burst_id_2")).otherwise(0)).alias("shell_burst_id_2"),
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"),
    )
)
