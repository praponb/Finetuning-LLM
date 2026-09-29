from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(F.max("event_time").alias("last_event_time"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(F.col("s.customer_id"), F.col("s.device"), F.col("ss.last_event_time"))
    .orderBy("last_event_time")
)
