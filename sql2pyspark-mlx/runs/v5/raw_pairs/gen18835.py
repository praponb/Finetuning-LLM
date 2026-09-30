from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k")
w5 = Window.partitionBy("k").orderBy("status_code_high", "id", "k")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

flattened_attrs = (
    lines
    .select(
        "id",
        "log_ts",
        "status_code",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

with_gaps = (
    flattened_attrs
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

prev_events = sessionized.withColumn("prev_log_ts", F.lag("log_ts").over(w))

with_gaps_2 = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized_app_logs = with_gaps_2.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessionized_app_logs
    .filter(F.col("is_new_session") >= 1)
    .select(
        "id",
        "log_ts",
        "k",
        "has_fault",
        "new_group_flag",
        "session_no",
        "prev_log_ts",
        "is_new_session",
        F.sum("new_group_flag").over(w3).alias("cumulative_new_group_flag"),
        F.count("status_code_high").over(w4).alias("k_count_status_code_high"),
        F.lag("status_code", 1).over(w5).alias("prev_status_code"),
    )
)
