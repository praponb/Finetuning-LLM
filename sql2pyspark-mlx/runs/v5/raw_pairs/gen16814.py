from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_totals.alias("session_totals"), F.col("sessions.id") == F.col("session_totals.session_id"), "inner")
    .filter(F.col("session_totals.unique_event_types") > 5)
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("session_totals.total_duration_sec"),
        F.col("session_totals.unique_event_types"),
    )
    .orderBy("total_duration_sec", "id")
    .limit(10)
)
