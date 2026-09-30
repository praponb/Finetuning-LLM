from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w4 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("k", "host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("k", "host").orderBy("running_count_session_no_2", "id", "k").rowsBetween(-2, -1)
w7 = Window.orderBy("running_max_session_no_2", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w9 = Window.partitionBy("host").orderBy(F.col("log_ts").desc(), "id", "k")
w10 = Window.partitionBy("k").orderBy("session_start_flag", "id", "k")
w11 = Window.partitionBy("k").orderBy("cumulative_session_no_2", "id", "k")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

gaps = (
    lines
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

gaps_2 = (
    sessions_app_logs
    .withColumn("is_new_burst", F.when(F.lag("log_ts").over(w3).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w3) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = (
    gaps_2
    .select(
        "id",
        "host",
        "log_ts",
        "k",
        "session_start_flag",
        "session_no",
        F.sum("is_new_burst").over(w4).alias("session_no_2"),
    )
)

with_windows = (
    islands
    .filter(F.col("session_start_flag") != 1)
    .withColumn("running_sum_session_no_2", F.sum("session_no_2").over(w_hist))
    .withColumn("running_count_session_no_2", F.count("session_no_2").over(w_hist))
    .withColumn("running_min_session_no_2", F.min("session_no_2").over(w_hist))
    .withColumn("running_max_session_no_2", F.max("session_no_2").over(w_hist))
)

with_windows_app_logs = (
    with_windows
    .select(
        "id",
        "k",
        "session_start_flag",
        "running_sum_session_no_2",
        "running_count_session_no_2",
        F.avg("running_count_session_no_2").over(w6).alias("running_count_session_no_2_avg_2_prior"),
        F.sum("session_no_2").over(w4).alias("cumulative_session_no_2"),
        F.last("running_sum_session_no_2", ignorenulls=True).over(w7).alias("last_known_running_sum_session_no_2"),
        F.count("session_start_flag").over(w_running).alias("session_start_flag_count_1h"),
        F.row_number().over(w9).alias("seq_no"),
    )
)

latest = (
    with_windows_app_logs
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    latest
    .withColumn("next_running_count_session_no_2_avg_2_prior", F.lead("running_count_session_no_2_avg_2_prior").over(w11))
    .withColumn("prev_session_start_flag", F.lag("session_start_flag").over(w11))
    .withColumn("next_session_start_flag", F.lead("session_start_flag").over(w11))
    .withColumn("event_seq", F.row_number().over(w11))
)

result = (
    metrics
    .withColumn("session_start_flag_tier", F.when(F.col("session_start_flag") >= 1, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["session_start_flag_tier", "seq_no"], ["session_start_flag_tier"], []],
        "session_start_flag_tier",
        "seq_no",
    )
    .agg(
        F.percentile_approx("event_seq", 0.9).alias("event_seq_p90"),
        F.max(F.when(F.col("k") == "version", F.col("event_seq"))).alias("version_max_event_seq"),
        F.countDistinct("k").alias("distinct_ks"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("distinct_ks").asc_nulls_last())
)
