from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.filter(F.col("started_at") < "2025-01-21").alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left_semi")
    .filter(F.col("web_events.event_time") >= "2023-05-18")
    .select(F.col("web_events.id"), F.col("web_events.event_type"))
    .orderBy("event_type")
)
