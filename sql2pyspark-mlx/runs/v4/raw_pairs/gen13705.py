from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-11-05 20:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select("*", F.col("txn_time").cast("date").alias("txn_time_day"))
)

prev_events = cleaned.withColumn("prev_txn_time", F.lag("txn_time").over(w))

flagged = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_transactions
    .groupBy(F.date_trunc("QUARTER", "txn_time").alias("quarter_start"))
    .agg(
        F.count("*").alias("event_count"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("session_id")).otherwise(0)).alias("debit_session_id"),
        F.max("session_id").alias("max_session_id"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("quarter_start").desc_nulls_first())
)
