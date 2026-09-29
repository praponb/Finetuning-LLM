from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("service")
w3 = Window.partitionBy("host")
w4 = Window.partitionBy("host").orderBy("log_ts", "id", "k")
w5 = Window.partitionBy("host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("service").orderBy("prev_log_ts", "id", "k")
w7 = Window.partitionBy("k").orderBy("prev_log_ts", "id", "k")
w8 = Window.partitionBy("k").orderBy("prev_log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

enriched = (
    lines
    .select(
        "*",
        F.first("k", ignorenulls=True).over(w).alias("first_known_k"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.corr("status_code", "latency_ms").over(w3).alias("corr_status_code_latency_ms"),
    )
)

prev_events = enriched.withColumn("prev_log_ts", F.lag("log_ts").over(w4))

with_gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w5))

enriched_app_logs = (
    sessions_app_logs
    .select(
        "*",
        F.row_number().over(w6).alias("row_seq"),
        F.lead("session_start_flag", 1, 0).over(w7).alias("next_session_start_flag"),
    )
)

flagged = (
    enriched_app_logs
    .withColumn("is_new_session", F.when(F.col("prev_log_ts") - F.lag("prev_log_ts").over(w7) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_app_logs_2 = flagged.withColumn("island_id", F.sum("is_new_session").over(w8))

result = (
    sessions_app_logs_2
    .groupBy(
        F.when(F.col("next_session_start_flag") >= 1, "HIGH").otherwise("LOW").alias("next_session_start_flag_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_rows"),
        F.sum(F.when(F.col("host") == "h3", F.col("latency_ms_share")).otherwise(0)).alias("h3_latency_ms_share"),
        F.percentile_approx("next_session_start_flag", [0.25, 0.5, 0.75]).alias("next_session_start_flag_quartiles"),
        F.sum(F.when(F.col("level") == "INFO", F.col("session_no"))).alias("info_total_session_no"),
    )
)
