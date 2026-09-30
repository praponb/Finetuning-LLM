from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.page") != "/home")
    .select(
        F.col("s.started_at"),
        F.col("we.event_time"),
        F.col("we.event_type"),
        F.col("we.session_id"),
    )
)
