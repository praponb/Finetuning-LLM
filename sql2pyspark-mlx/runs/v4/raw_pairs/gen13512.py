from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("host").orderBy("session_id", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("host").orderBy("session_start_flag", "id", "k")
w5 = Window.partitionBy("k").orderBy("session_start_flag", "id", "k")
w6 = Window.partitionBy("service").orderBy(F.col("running_status_code").asc_nulls_first(), "id", "k")
w7 = Window.partitionBy("host").orderBy("log_ts", "id", "k")
w8 = Window.partitionBy("host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w10 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("service").orderBy(F.col("session_start_flag_bucket").asc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

prev_events = flattened.withColumn("prev_log_ts", F.lag("log_ts").over(w))

flagged = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

metrics = (
    sessions_app_logs
    .select(
        "*",
        F.sum("status_code").over(w_recent).alias("running_status_code"),
        F.lead("session_start_flag", 1).over(w4).alias("next_session_start_flag"),
        F.ntile(5).over(w5).alias("session_start_flag_bucket"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

with_gaps = (
    deduped
    .withColumn("session_start_flag_2", F.when(F.lag("log_ts").over(w7).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w7) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_app_logs_2 = with_gaps.withColumn("burst_id", F.sum("session_start_flag_2").over(w8))

flagged_2 = (
    sessions_app_logs_2
    .withColumn("session_start_flag_3", F.when(F.lag("log_ts").over(w9).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w9) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = flagged_2.withColumn("island_id", F.sum("session_start_flag_3").over(w10))

result = (
    islands
    .select(
        "*",
        F.rank().over(w11).alias("session_start_flag_bucket_rank"),
        F.first("running_status_code", ignorenulls=True).over(w10).alias("first_known_running_status_code"),
    )
)
