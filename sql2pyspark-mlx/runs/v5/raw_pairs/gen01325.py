from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy(F.col("avg_session_id").desc())

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("Lazada"))))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("UNKNOWN")))
)

prev_events = cleaned.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    with_gaps
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "resolved_merchant",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

result = (
    sessions_transactions
    .groupBy("session_id", F.date_trunc("YEAR", "txn_time").alias("year_start"))
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
    )
    .withColumn("avg_session_id_rank", F.rank().over(w3))
)
