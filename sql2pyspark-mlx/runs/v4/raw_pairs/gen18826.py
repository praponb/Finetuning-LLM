from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key").orderBy("prev_log_ts", "id", "key")
w4 = Window.partitionBy("key").orderBy("prev_log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("key").orderBy(F.col("flag_bit1").desc(), "id", "key")
w_running = Window.partitionBy("key").orderBy(F.col("burst_id").desc())
w7 = Window.partitionBy("key")
w8 = Window.partitionBy("key").orderBy("is_new_burst", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

flattened_attrs = (
    flattened
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)

base = (
    flattened_attrs
    .filter((F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 08:00:00"))) & (F.col("key") == "version"))
    .select("*", F.coalesce("key", F.lit("unassigned")).alias("key_filled"))
)

prev_events = base.withColumn("prev_log_ts", F.lag("log_ts").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

with_gaps_2 = (
    islands
    .withColumn("is_new_session", F.when(F.lag("prev_log_ts").over(w3).isNull() | (F.col("prev_log_ts") > F.lag("prev_log_ts").over(w3) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_app_logs = with_gaps_2.withColumn("session_no", F.sum("is_new_session").over(w4))

scored = (
    sessions_app_logs
    .withColumn("flag_bit1_bucket", F.ntile(4).over(w5))
    .withColumn("burst_id_rank", F.rank().over(w_running))
    .withColumn("burst_id_dense_rank", F.dense_rank().over(w_running))
    .withColumn("key_stddev_samp_status_code_high", F.stddev_samp("status_code_high").over(w7))
    .withColumn("prev_burst_id", F.lag("burst_id", 1).over(w8))
)

with_prev = scored.withColumn("prev_prev_log_ts", F.lag("prev_log_ts").over(w3))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_prev_log_ts").isNull() | (F.col("prev_log_ts") > F.col("prev_prev_log_ts") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_app_logs_2 = flagged.withColumn("session_no_2", F.sum("session_start_flag").over(w4))

result = (
    sessions_app_logs_2
    .select(
        "*",
        F.ntile(3).over(w5).alias("flag_bit1_bucket_2"),
        F.sum("flag_bit1_bucket").over(w7).alias("key_sum_flag_bit1_bucket"),
    )
)
