from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.min("event_time").alias("min_event_time"),
        F.max("event_time").alias("latest_event_time"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_totals.alias("SessionTotals"), F.col("sessions.id") == F.col("SessionTotals.session_id"), "left")
    .select(
        F.col("sessions.id"),
        F.col("sessions.device"),
        F.col("sessions.country"),
        F.col("SessionTotals.min_event_time"),
        F.col("SessionTotals.latest_event_time"),
    )
)
