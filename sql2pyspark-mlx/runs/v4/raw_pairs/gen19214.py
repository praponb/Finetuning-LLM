from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "attr_key").orderBy("log_ts", "id", "attr_key")
w_ordered = Window.orderBy("status_code_high", "id", "attr_key")
w3 = Window.partitionBy("service").orderBy(F.col("log_ts").desc(), "id", "attr_key")
w4 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key")
w5 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("service", "attr_key").orderBy(F.col("status_code_high").desc_nulls_last())
w_hist = Window.partitionBy("service").orderBy("session_start_flag", "id", "attr_key").rowsBetween(-6, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

flattened_attrs = (
    flattened
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "attr_key",
        "value",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    flattened_attrs
    .withColumn("next_status_code_high", F.lead("status_code_high", 1).over(w))
    .withColumn("seq_no", F.row_number().over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w3))
)

with_gaps = (
    with_windows
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w4).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w4) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_app_logs = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w5))

enriched = (
    sessions_app_logs
    .withColumn("status_code_high_cume_dist", F.cume_dist().over(w_grp))
    .withColumn("session_start_flag_avg_6", F.avg("session_start_flag").over(w_hist))
    .withColumn("event_seq_2", F.row_number().over(w_ordered))
)

result = (
    enriched
    .select(
        "id",
        "service",
        "status_code",
        "attr_key",
        "has_fault",
        "status_code_high",
        "session_start_flag",
        "session_id",
        "status_code_high_cume_dist",
    )
    .orderBy(F.col("service").desc_nulls_last())
)
