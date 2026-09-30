from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.hour("event_time").between(10, 16)).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.browser"), F.col("s.device"), F.col("s.started_at"))
    .orderBy("started_at")
)
