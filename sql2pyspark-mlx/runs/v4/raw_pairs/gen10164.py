from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.hour("event_time").between(12, 17))
    .groupBy("session_id")
    .agg(F.min("event_time").alias("min_event_time"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .select(F.col("s.customer_id"), F.col("t.min_event_time"))
    .orderBy("min_event_time")
)
