from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("duration_sec").between(435, 443))
    .groupBy("session_id")
    .agg(F.sort_array(F.collect_set("event_type")).alias("event_type_list"))
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("sessions.device"),
        F.col("sessions.started_at"),
        F.col("t.event_type_list"),
    )
    .orderBy(F.col("device").desc())
)
