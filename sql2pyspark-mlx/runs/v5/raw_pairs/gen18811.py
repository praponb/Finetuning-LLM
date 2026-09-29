from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.session_id") == 16)
    .groupBy(F.col("s.country"))
    .agg(F.min(F.col("we.event_time")).alias("min_event_time"))
    .orderBy(F.col("min_event_time").desc())
)
