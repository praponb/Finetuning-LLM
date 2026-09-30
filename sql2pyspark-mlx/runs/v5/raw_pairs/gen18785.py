from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.duration_sec"),
        F.col("s.country"),
        F.col("s.browser"),
        F.col("s.device"),
    )
    .orderBy("country", "id")
    .limit(5)
)
