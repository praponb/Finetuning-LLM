from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.min("event_time").alias("min_event_time"),
        F.sum(F.when(F.col("event_type") == "page_view", F.col("duration_sec")).otherwise(0)).alias("page_view_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_totals.alias("session_totals"), F.col("sessions.id") == F.col("session_totals.session_id"), "inner")
    .select(
        F.col("sessions.id"),
        F.col("sessions.customer_id"),
        F.col("sessions.started_at"),
        F.col("session_totals.min_event_time"),
        F.col("session_totals.page_view_duration_sec"),
    )
)
