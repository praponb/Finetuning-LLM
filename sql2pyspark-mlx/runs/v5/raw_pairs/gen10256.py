from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("prev_event_time", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("event_type").orderBy("session_start_flag", "id")
w5 = Window.partitionBy("session_id").orderBy("session_id_2", "id")
w6 = Window.orderBy("page_length_avg_3", "id")
w7 = Window.partitionBy("session_id").orderBy("prev_event_time", "id")
w8 = Window.partitionBy("session_id").orderBy("prev_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

cleaned = (
    df_web_events
    .filter(F.col("event_time") > F.to_date(F.lit("2023-08-10")))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.length("page").alias("page_length"),
    )
)

with_prev = cleaned.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("session_id_2", F.sum("session_start_flag").over(w2))

scored = (
    sessions_web_events
    .select(
        "id",
        "session_id",
        "prev_event_time",
        "session_id_2",
        F.avg("page_length").over(w_hist).alias("page_length_avg_3"),
        F.ntile(5).over(w4).alias("session_start_flag_bucket"),
    )
)

with_windows = (
    scored
    .withColumn("next_session_id_2", F.lead("session_id_2").over(w5))
    .withColumn("prev_session_id_2", F.lag("session_id_2").over(w5))
    .withColumn("next_page_length_avg_3", F.lead("page_length_avg_3").over(w5))
    .withColumn("prev_page_length_avg_3", F.lag("page_length_avg_3").over(w5))
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("prev_session_id_2_2", F.lag("session_id_2", 1).over(w6))
)

flagged = (
    with_windows
    .withColumn("new_group_flag", F.when(F.lag("prev_event_time").over(w7).isNull() | (F.col("prev_event_time") > F.lag("prev_event_time").over(w7) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_group_flag").over(w8))

enriched = (
    sessionized
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "left")
    .filter(F.col("p2.device") != "mobile")
    .select(
        "s.id",
        "s.session_id",
        "s.next_session_id_2",
        "s.prev_session_id_2",
        "s.prev_page_length_avg_3",
        "s.seq_no",
        F.col("p2.browser"),
    )
)

result = (
    enriched
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "prev_session_id_2",
        "prev_page_length_avg_3",
        "seq_no",
        "browser",
        F.col("avg_web_event_duration_sec"),
    )
)
