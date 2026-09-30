from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("page") == "/checkout")
    .groupBy("session_id")
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.stddev_duration_sec") > 76)
    .select(
        F.col("sessions.browser"),
        F.col("t.event_type_list"),
        F.col("t.stddev_duration_sec"),
    )
    .orderBy("stddev_duration_sec")
)
