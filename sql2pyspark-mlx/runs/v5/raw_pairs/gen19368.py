from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "k").orderBy("log_ts")
w2 = Window.partitionBy("host").orderBy("log_ts", "id", "k")
w3 = Window.partitionBy("host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("host").orderBy("status_code", "id", "k")
w5 = Window.partitionBy("k").orderBy("new_group_flag", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

deduped = (
    flattened
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

flagged = (
    deduped
    .withColumn("new_group_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w2) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_app_logs = flagged.withColumn("session_id", F.sum("new_group_flag").over(w3))

metrics = (
    sessions_app_logs
    .withColumn("prev_status_code", F.lag("status_code").over(w_running))
    .withColumn("prev_session_id", F.lag("session_id").over(w_running))
    .withColumn("next_session_id", F.lead("session_id").over(w_running))
    .withColumn("seq_no", F.row_number().over(w_running))
    .withColumn("running_latency_ms", F.sum("latency_ms").over(w5))
)

totals = (
    metrics
    .groupBy(
        F.date_trunc("hour", "log_ts").alias("hour_window"),
        F.when(F.col("next_session_id") >= 6, "HIGH").otherwise("LOW").alias("next_session_id_tier"),
    )
    .agg(
        F.count_if(F.col("k") == "trace").alias("trace_events"),
        F.max(F.when(F.col("k") == "trace", F.col("latency_ms"))).alias("trace_max_latency_ms"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("trace_max_latency_ms") < 1000000)
)

result = (
    totals
    .select("hour_window", "next_session_id_tier", "trace_events", "row_count")
    .orderBy(
        F.col("next_session_id_tier").desc_nulls_first(),
        F.col("trace_events").desc(),
    )
)
