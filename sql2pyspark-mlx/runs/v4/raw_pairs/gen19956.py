from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("level").orderBy("latency_ms", "id", "key")
w3 = Window.partitionBy("key").orderBy("latency_ms")
w4 = Window.orderBy("log_ts", "id", "key")
w5 = Window.partitionBy("level").orderBy("log_ts", "id", "key")
w6 = Window.partitionBy("level").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w8 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("key").orderBy(F.col("is_new_session").asc_nulls_first(), "id", "key")
w10 = Window.partitionBy("key", "level").orderBy("status_code_share", "id", "key")
w11 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w12 = Window.partitionBy("level").orderBy("is_new_session")
w13 = Window.partitionBy("level").orderBy("status_code_share", "id", "key")
w14 = Window.partitionBy("key", "level").orderBy("new_group_flag", "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

with_windows = (
    exploded
    .withColumn("level_min_latency_ms", F.min("latency_ms").over(w))
    .withColumn("next_latency_ms", F.lead("latency_ms", 2, 0).over(w2))
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w3))
    .withColumn("next_latency_ms_2", F.lead("latency_ms", 2, 0).over(w4))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
)

with_gaps = (
    with_windows
    .withColumn("new_group_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w5) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessions_app_logs = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w6))

with_gaps_2 = (
    sessions_app_logs
    .withColumn("is_new_session", F.when(F.col("log_ts") - F.lag("log_ts").over(w7) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessionized = with_gaps_2.withColumn("session_id", F.sum("is_new_session").over(w8))

deduped = (
    sessionized
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    deduped
    .withColumn("event_seq", F.row_number().over(w10))
    .withColumn("next_latency_ms_2_count_30m", F.count("next_latency_ms_2").over(w11))
    .withColumn("is_new_session_cume_dist", F.cume_dist().over(w12))
)

metrics = (
    scored
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("next_session_id", F.lead("session_id", 1, 0).over(w13))
    .withColumn("event_seq_2", F.row_number().over(w14))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("latency_ms_share") >= 0.61, "LARGE").otherwise("SMALL").alias("latency_ms_share_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("level") == "INFO", 1)).alias("info_rows"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count_if(F.col("level") == "ERROR").alias("error_events"),
        F.percentile_approx("new_group_flag", [0.25, 0.5, 0.75]).alias("new_group_flag_quartiles"),
    )
    .orderBy(F.col("num_levels").desc())
)
