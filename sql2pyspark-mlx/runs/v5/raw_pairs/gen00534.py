from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service", "host").orderBy("burst_id", "id", "attr_key").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("service").orderBy("new_group_flag", "id", "attr_key")
w5 = Window.partitionBy("host").orderBy("log_ts", "id", "attr_key")
w6 = Window.partitionBy("host").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("attr_key").orderBy(F.col("version_rows").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

with_gaps = (
    flattened
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

with_windows = (
    sessionized
    .select(
        "*",
        F.avg("status_code").over(w3).alias("last7_avg_status_code"),
        F.min("status_code").over(w3).alias("last7_min_status_code"),
        F.sum("status_code").over(w3).alias("last7_sum_status_code"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

prev_events = with_windows.withColumn("prev_log_ts", F.lag("log_ts").over(w5))

flagged = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = flagged.withColumn("session_no", F.sum("is_new_session").over(w6))

result = (
    sessions_app_logs
    .groupBy("attr_key", "session_no")
    .agg(
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_rows"),
        F.count(F.when(F.col("attr_key") == "version", 1)).alias("version_rows"),
        F.countDistinct("service").alias("distinct_services"),
        F.round(F.sum("new_group_flag") / F.nullif(F.count(F.when(F.col("host") == "h2", 1)), F.lit(0)), 2).alias("new_group_flag_per_h2"),
        F.percentile_approx("status_code", 0.9).alias("status_code_p90"),
    )
    .withColumn("version_rows_dense_rank", F.dense_rank().over(w7))
    .orderBy(F.col("session_no").asc_nulls_first())
)
